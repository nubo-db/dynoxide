//! Key re-derivation from a data model's entity templates.
//!
//! In a single-table design the key attributes are built from other
//! attributes (`user#${email}`), so anonymising `email` on its own leaves the
//! real value sitting in `sk` and every GSI key built from it. With a data
//! model loaded, the importer works out which of an item's keys its entity
//! builds from templates, lets the rules rewrite the attributes, then renders
//! those keys again from the anonymised attributes. The entity prefix
//! survives, and key and attribute agree by construction.
//!
//! A key is only rewritten when its template reproduces the value the item
//! arrived with. A key the template cannot reproduce is left as it is and
//! reported once per entity and key, so a template that does not describe
//! the data never silently rewrites a key. Warnings never quote a key's
//! value: the whole point of the run is that those values leave.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::actions::create_table::CreateTableRequest;
use crate::expressions::{PathElement, resolve_path};
use crate::schema::{DataModel, EntityDefinition};
use crate::types::{AttributeValue, Item};
use crate::validation::{partition_key_name, sort_key_name};

use super::config::{ValidatedAction, ValidatedRule, parse_path};
use super::notice::Notice;

/// Rebuilt primary keys are remembered (as hashes) to spot two items
/// collapsing onto one row. Past this many the check stops, and says so.
pub(super) const MAX_TRACKED_KEYS: usize = 1_000_000;

/// Longest `${name:length:pad}` padding a key is allowed to ask for. DynamoDB
/// caps a partition key at 2048 bytes and every character costs at least one,
/// so a longer pad cannot produce a key the table would accept, and asking for
/// one is a typo rather than intent.
const MAX_PAD_LENGTH: usize = 2048;

/// One piece of a `${name}` template.
#[derive(Debug, Clone, PartialEq)]
enum Segment {
    Literal(String),
    /// A `${path}` reference, resolved against the item like an attribute
    /// path (`address.city` reaches into a map), with OneTable's optional
    /// `${path:length:pad}` sort padding.
    Var {
        path: Vec<PathElement>,
        pad: Option<Padding>,
    },
}

/// OneTable's `${name:length:pad}` padding, used to keep numbers sortable
/// as strings. The rendered value is prefixed with `fill` until it is at
/// least `length` characters, and `fill` defaults to `0`.
#[derive(Debug, Clone, PartialEq)]
struct Padding {
    length: usize,
    fill: String,
}

impl Padding {
    /// Pad to `length` characters, matching JavaScript's `padStart`, which is
    /// what OneTable itself renders with.
    ///
    /// The fill repeats and is then cut to the exact shortfall, so a
    /// multi-character fill lands on `length` rather than overshooting it.
    /// Prepending the fill whole until the value was long enough rendered
    /// `${orderId:4:00}` over `7` as `00007` where OneTable gives `0007`, and
    /// a key the template cannot reproduce is left unrebuilt, keeping the
    /// value it arrived with. The single-character default hid that.
    fn apply(&self, value: &str) -> String {
        if self.fill.is_empty() {
            return value.to_string();
        }
        // Counted in UTF-16 code units, because that is what JavaScript's
        // `length` counts and the model was written against it: a character
        // outside the basic plane is two units there and one `char` here, so
        // counting chars pads an emoji one short of what OneTable renders.
        // The fill is still cycled by character, so a fill that is itself
        // astral and a shortfall that is odd cannot be split the way
        // JavaScript would; a fill outside the basic plane is not a shape
        // OneTable's numeric padding produces.
        let have = value.encode_utf16().count();
        if have >= self.length {
            return value.to_string();
        }
        let shortfall = self.length - have;
        let mut out = String::new();
        let mut units = 0usize;
        for c in self.fill.chars().cycle() {
            let width = c.len_utf16();
            if units + width > shortfall {
                break;
            }
            out.push(c);
            units += width;
        }
        out.push_str(value);
        out
    }
}

/// Split a template such as `user#${email}` into literal and variable parts.
///
/// Follows OneTable's own `${name}` and `${name:length:pad}` forms, and its
/// dotted paths. An unclosed `${` is rejected rather than treated as text,
/// since it would otherwise leave a key untracked without a word said.
fn parse_template(template: &str) -> Result<Vec<Segment>, String> {
    let mut segments = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        let Some(len) = rest[start..].find('}') else {
            return Err(format!("template '{template}' has an unclosed '${{'"));
        };
        if start > 0 {
            segments.push(Segment::Literal(rest[..start].to_string()));
        }
        let var = &rest[start + 2..start + len];

        let mut parts = var.split(':');
        let name = parts.next().unwrap_or(var);
        let pad = match parts.next() {
            Some(length) => {
                let length: usize = length.trim().parse().map_err(|_| {
                    format!(
                        "template '{template}': '{length}' in '${{{var}}}' is not a length; \
                         the form is '${{name:length:pad}}'"
                    )
                })?;
                if length > MAX_PAD_LENGTH {
                    return Err(format!(
                        "template '{template}': a pad length of {length} is beyond the {MAX_PAD_LENGTH} \
                         a key can usefully hold"
                    ));
                }
                // OneTable treats an empty pad character as "0" (`pad || '0'`),
                // and an empty one would also never reach the length.
                let fill = match parts.next() {
                    Some("") | None => "0".to_string(),
                    Some(fill) => fill.to_string(),
                };
                Some(Padding { length, fill })
            }
            None => None,
        };

        let path = parse_path(name)
            .map_err(|e| format!("template '{template}' has an invalid reference '{name}': {e}"))?;
        segments.push(Segment::Var { path, pad });
        rest = &rest[start + len + 1..];
    }
    if !rest.is_empty() {
        segments.push(Segment::Literal(rest.to_string()));
    }
    Ok(segments)
}

/// Render a template from the item's attributes. `None` when a referenced
/// attribute is missing or is not a scalar the key can hold.
fn render(segments: &[Segment], item: &Item) -> Option<String> {
    let mut out = String::new();
    for segment in segments {
        match segment {
            Segment::Literal(s) => out.push_str(s),
            Segment::Var { path, pad } => {
                let rendered = match resolve_path(item, path)? {
                    AttributeValue::S(s) => s,
                    AttributeValue::N(n) => n,
                    AttributeValue::BOOL(b) => if b { "true" } else { "false" }.to_string(),
                    _ => return None,
                };
                match pad {
                    Some(pad) => out.push_str(&pad.apply(&rendered)),
                    None => out.push_str(&rendered),
                }
            }
        }
    }
    Some(out)
}

/// A key attribute an entity builds from a template with at least one variable.
#[derive(Debug, Clone)]
struct TemplatedKey {
    attribute: String,
    template: String,
    segments: Vec<Segment>,
}

impl TemplatedKey {
    /// The attribute paths the template reads, in full, so a nested
    /// `${contact.email}` is followed rather than collapsed to `contact`.
    fn source_paths(&self) -> impl Iterator<Item = &Vec<PathElement>> {
        self.segments.iter().filter_map(|s| match s {
            Segment::Var { path, .. } => Some(path),
            Segment::Literal(_) => None,
        })
    }

    /// Top-level attribute names the template reads.
    fn sources(&self) -> impl Iterator<Item = &str> {
        self.source_paths().filter_map(|path| match path.first() {
            Some(PathElement::Attribute(name)) => Some(name.as_str()),
            _ => None,
        })
    }
}

/// The templated keys of one entity, resolved against one table's key schema.
#[derive(Debug, Clone)]
struct EntityKeys {
    name: String,
    type_attribute: String,
    /// Keys with at least one variable: the ones worth rebuilding.
    keys: Vec<TemplatedKey>,
    /// Every key the entity templates, constant ones included. Used to tell
    /// entities apart when an item carries no type attribute, where a
    /// constant `sk = "PROFILE"` is often the only thing separating two
    /// entities that share a partition template.
    match_keys: Vec<TemplatedKey>,
}

/// A key attribute at least one entity builds from something a rule rewrites.
///
/// The join check has three rules, and this is the one place they are stated.
///
/// **What is compared.** A key that will change: one its template rebuilds
/// from an anonymised attribute, or one a rule names directly. A key left
/// holding its original value is reported as unrebuilt and is not compared,
/// since that is the accurate diagnostic and [consistency] cannot fix it.
///
/// **What is grouped.** A partition key groups rows by its own value. A sort
/// key only orders rows inside a partition it does not define, so it is
/// bucketed together with that partition's value, one bucket per index it
/// sorts, with the index's partition attribute named in the bucket so two
/// indexes holding the same string stay apart. `pk` and `sk` are unique
/// together on a base table, so a base sort key never groups two rows; a GSI
/// sort key is not unique and can.
///
/// **What is fatal.** Two entities that arrived sharing an original key and
/// leave with different ones have broken their join: the import fails. One
/// entity that takes one original key to two values has split the rows that
/// shared it. On a partition key that is the same broken join and fails; on
/// a sort key it is reported and the run continues, because rows share a
/// sort value for reasons that are not relationships and failing there would
/// push people to drop the data model and ship the real values.
///
/// One entity is enough to form a group. Roots already listed in
/// `[consistency] fields` are tracked too rather than excused, since the map
/// stops taking new values at its cap and starts handing out fresh ones, and
/// the value check is what notices when that happens. Anonymising it independently per item means the
/// entities' keys disagree and the join between them is lost.
#[derive(Debug)]

struct AtRisk {
    /// The key attribute the shared template builds.
    key_attribute: String,
    /// The attributes whose values decide which rows this key groups with,
    /// when it does not group by itself. Empty for a partition key, which is
    /// its own grouping. One entry per index a sort key sorts, because each
    /// index groups rows differently and each is a join that can break on its
    /// own.
    grouped_by: Vec<String>,
    /// Root field names to put in `[consistency] fields`. Roots, because
    /// that is what the consistency map is keyed on: advising `contact.email`
    /// when only `contact` is honoured would send someone in a circle.
    consistency_roots: Vec<String>,
    /// Entity indices that build this key from an attribute a rule rewrites,
    /// in model order. These are the entities the up-front warning names.
    entities: Vec<usize>,
    /// Entity indices that build the same key from an attribute no rule
    /// rewrites. Their keys keep the values they arrived with, so they are
    /// what a rewritten sibling has to keep agreeing with: a `Customer`
    /// anonymised on `email` beside an `Order` keyed on an untouched
    /// `customerEmail` is the join that breaks, and it is only found by
    /// recording what the untouched entity's key stayed as.
    observers: Vec<usize>,
    /// Hash of the whole key value the item arrived with -> every (entity,
    /// resulting key value) seen for it. Comparing the whole key, rather than
    /// one attribute it reads, tells a real join from two composite keys that
    /// merely share a component: `TENANT#a#x` and `TENANT#b#x` never agreed,
    /// so they have nothing to lose. A deterministic action such as hash
    /// takes both to the same place and stays quiet.
    seen_keys: std::collections::HashMap<u64, Vec<Outcome>>,
    /// Entities found to have taken a shared key to different values.
    diverged: HashSet<usize>,
    /// Entities that split a key of their own which only orders rows inside a
    /// partition it does not define. Reported rather than fatal: rows share a
    /// sort value for reasons that are not relationships.
    split_alone: HashSet<usize>,
    /// Keys that went unchecked after the cap.
    unchecked: usize,
}

/// What one entity made of one original key value.
///
/// A summary rather than every value: many items of one entity sharing a
/// customer key would otherwise keep a row each, and be rescanned per item.
/// The first result, plus whether that entity ever produced a second, is
/// enough to spot a disagreement with another entity.
#[derive(Debug, Clone, Copy)]
struct Outcome {
    entity: usize,
    first: u64,
    /// This entity produced more than one distinct result for the key.
    multiple: bool,
}

/// Which of an item's keys can be rebuilt after the rules run.
#[derive(Debug)]
pub struct Rederivation {
    entity: usize,
    keys: Vec<usize>,
    /// Scalar values of the rule-target attributes as the item arrived, so
    /// `apply` can see whether a key the model never templated still holds
    /// one after the rules replaced it.
    originals: Vec<(String, String)>,
    /// (at-risk index, hash of the value this item arrived with), so `apply`
    /// can see whether two entities anonymised a shared value differently.
    at_risk_originals: Vec<(usize, u64)>,
}

/// Rebuilds templated keys for the items of one table.
#[derive(Debug)]
pub struct KeyDeriver {
    entities: Vec<EntityKeys>,
    /// Every distinct type attribute the entities use, usually just the
    /// model's default, so an item is looked up once per attribute rather
    /// than once per entity.
    type_attributes: Vec<String>,
    /// Primary key attribute names, for the collision check.
    hash_attribute: Option<String>,
    range_attribute: Option<String>,
    /// (entity index, key index) pairs whose template failed to reproduce
    /// a value, reported once so a whole table of off-template items
    /// produces one warning rather than one per item.
    warned_mismatch: HashMap<(usize, usize), usize>,
    /// Key attributes a rule names directly, so `plan` can tell a template
    /// that failed from a key the rules were always going to replace.
    rule_targets: HashSet<String>,
    /// Every key attribute the table and its indexes have, so `apply` can
    /// find the ones an entity carries but its model never templates.
    schema_keys: Vec<String>,
    /// The full path of every rule that applies to this table, so `plan` can
    /// read the value a nested rule is about to replace, not only a
    /// top-level one.
    rule_paths: Vec<Vec<PathElement>>,
    /// (entity, key attribute) -> items whose untemplated key still held a
    /// value a rule had replaced. Warned once, counted throughout.
    survived: HashMap<(usize, String), usize>,
    /// (entity index, key index) pairs that could not be rendered after the
    /// rules ran, reported once.
    warned_unrenderable: HashMap<(usize, usize), usize>,
    /// Items that matched no entity, reported once per table.
    unmatched: usize,
    /// (entity index, key index) pairs where a rule took the key instead of
    /// its template, reported once.
    warned_rule_wins: HashSet<(usize, usize)>,
    /// Sets of entities that all reproduced some untyped item's keys,
    /// reported once per set.
    warned_ambiguous: HashSet<Vec<usize>>,
    /// Attribute -> the entities that build a key from it, for attributes
    /// more than one entity keys on that are not consistency-tracked. Their
    /// keys cannot agree, so the entities stop joining.
    at_risk: Vec<AtRisk>,
    /// Hashes of every rebuilt primary key, to spot collisions.
    rebuilt_keys: HashSet<u64>,
    /// Rebuilt items whose primary key repeated an earlier rebuilt key.
    collisions: usize,
    /// Set once `rebuilt_keys` hit its cap and stopped tracking.
    collision_check_capped: bool,
}

impl KeyDeriver {
    /// Resolve the model's templates against the table's key schema: the
    /// primary key from `KeySchema`, and each GSI mapping against the index
    /// of the same name. Templates without a variable never change, so they
    /// are not tracked.
    ///
    /// The rules are consulted too. A rule that targets a key attribute
    /// directly wins over the template, and that key is not rebuilt. A rule
    /// that redacts, nulls or masks an attribute a key is built from is
    /// reported, because every item would then render the same key and
    /// collapse onto one row. Both are returned as warnings alongside the
    /// deriver; a template that cannot be parsed is an error.
    pub fn new(
        model: &DataModel,
        request: &CreateTableRequest,
        rules: &[ValidatedRule],
        consistency_fields: &HashSet<String>,
    ) -> Result<(Self, Vec<Notice>), String> {
        let hash = partition_key_name(&request.key_schema);
        let range = sort_key_name(&request.key_schema);
        let gsis = request.global_secondary_indexes.as_deref().unwrap_or(&[]);

        // What groups rows with each other, per key attribute. A partition key
        // groups by itself: rows sharing one come back from a single query. A
        // sort key only orders rows inside a partition it does not define, so
        // two rows sharing a sort value are grouped only when they share that
        // partition as well, and the join check has to read both. Comparing a
        // sort value alone calls two rows a group when they were never in one
        // place, and splitting them then reads as a join break that never
        // existed. An attribute that is a partition key anywhere groups by
        // itself, whatever else it sorts.
        let mut schema_keys: Vec<String> = Vec::new();
        for name in hash.into_iter().chain(range) {
            schema_keys.push(name.to_string());
        }
        for index in gsis {
            for ks in &index.key_schema {
                if !schema_keys.contains(&ks.attribute_name) {
                    schema_keys.push(ks.attribute_name.clone());
                }
            }
        }
        for index in request.local_secondary_indexes.as_deref().unwrap_or(&[]) {
            for ks in &index.key_schema {
                if !schema_keys.contains(&ks.attribute_name) {
                    schema_keys.push(ks.attribute_name.clone());
                }
            }
        }

        let mut partition_keys: HashSet<&str> = HashSet::new();
        partition_keys.extend(hash);
        for gsi in gsis {
            partition_keys.extend(partition_key_name(&gsi.key_schema));
        }
        // One attribute can sort more than one index, and each index is its
        // own grouping with its own join to lose. Keeping only the first
        // partition found compared rows against one index and left a split
        // inside any other index unexamined.
        let mut grouped_by: HashMap<&str, Vec<&str>> = HashMap::new();
        if let (Some(range), Some(hash)) = (range, hash) {
            grouped_by.entry(range).or_default().push(hash);
        }
        for gsi in gsis {
            if let (Some(sort), Some(part)) = (
                sort_key_name(&gsi.key_schema),
                partition_key_name(&gsi.key_schema),
            ) {
                let partitions = grouped_by.entry(sort).or_default();
                if !partitions.contains(&part) {
                    partitions.push(part);
                }
            }
        }
        grouped_by.retain(|attribute, _| !partition_keys.contains(attribute));

        // Only the rules that apply to this table. A rule scoped elsewhere
        // that names `pk` made this table's `pk` look rule-owned, so a key
        // the template could not rebuild went uncounted and a shared key a
        // direct rule never touched here left the join check.
        let rules: Vec<&ValidatedRule> = rules
            .iter()
            .filter(|rule| rule.applies_to(&request.table_name))
            .collect();
        let rule_targets: HashSet<&str> = rules.iter().filter_map(|r| rule_target(r)).collect();
        let rule_paths: Vec<Vec<PathElement>> = rules.iter().map(|r| r.path.clone()).collect();
        let mut warnings: Vec<Notice> = Vec::new();
        let mut unmatched_indexes: HashSet<String> = HashSet::new();

        // A local secondary index never reaches the model: OneTable declares
        // one with a sort key and no hash, and the parser keeps only indexes
        // that name a hash attribute. Say so rather than leave its sort key
        // quietly holding the value it arrived with.
        // A warning rather than an exposure: the model says nothing about an
        // LSI, so nothing says its sort key embeds an attribute a rule
        // touches. An index the model does describe but the table lacks is
        // different, and stays an exposure below, because there the model
        // itself claims those keys are built from attributes.
        if let Some(lsis) = request.local_secondary_indexes.as_deref()
            && !lsis.is_empty()
        {
            warnings.push(Notice::caution(format!(
                "has {} local secondary index(es); their sort keys are not rebuilt from \
                 templates and keep the values they arrive with",
                lsis.len()
            )));
        }

        let mut entities = Vec::with_capacity(model.entities.len());
        type KeyShape = (String, String, Vec<Vec<PathElement>>);
        let mut entity_key_shapes: Vec<Vec<KeyShape>> = Vec::with_capacity(model.entities.len());
        for entity in &model.entities {
            let mut match_keys = Vec::new();
            push_key(&mut match_keys, entity, hash, Some(&entity.pk_template))?;
            push_key(
                &mut match_keys,
                entity,
                range,
                entity.sk_template.as_deref(),
            )?;

            for mapping in &entity.gsi_mappings {
                let Some(gsi) = gsis.iter().find(|g| g.index_name == mapping.index_name) else {
                    // OneTable defaults an index's name to its schema key
                    // ("gs1"), which rarely matches the deployed index
                    // ("GSI1"). Silently skipping would leave that index's
                    // key holding whatever it arrived with.
                    if unmatched_indexes.insert(mapping.index_name.clone()) {
                        // The model itself says this index's keys are built
                        // from attributes, so their keeping the values they
                        // arrived with is an observed survival, not a guess.
                        warnings.push(Notice::exposure(format!(
                            "the data model has an index '{}' that the table does not: its \
                             keys are not rebuilt and keep the values they arrive with. Set \
                             the OneTable index's \"name\" to the DynamoDB index name",
                            mapping.index_name
                        )));
                    }
                    continue;
                };
                push_key(
                    &mut match_keys,
                    entity,
                    partition_key_name(&gsi.key_schema),
                    Some(&mapping.pk_template),
                )?;
                push_key(
                    &mut match_keys,
                    entity,
                    sort_key_name(&gsi.key_schema),
                    mapping.sk_template.as_deref(),
                )?;
            }

            // A key with no `${...}` in it is not rebuilt, because there is
            // nothing in it to rebuild from. It is still a key a rule naming
            // the attribute directly can rewrite, though, and two entities
            // sharing a constant partition such as `TENANT#` are as joined as
            // two sharing a templated one. Keeping those here lets the join
            // check see them; `plan` still only rebuilds a key that has a
            // variable to render.
            let keys: Vec<TemplatedKey> = match_keys
                .iter()
                .filter(|key| {
                    key.segments
                        .iter()
                        .any(|s| matches!(s, Segment::Var { .. }))
                        || rule_targets.contains(key.attribute.as_str())
                })
                .cloned()
                .collect();

            // A key built from another templated key would have to be
            // rendered in dependency order, and rendering it first copies the
            // pre-anonymisation value. Refuse rather than leave that to luck.
            for key in &keys {
                for source in key.sources() {
                    if keys
                        .iter()
                        .any(|other| other.attribute == source && other.attribute != key.attribute)
                    {
                        return Err(format!(
                            "entity '{}': template '{}' for {} reads key attribute '{}', which is \
                             itself built from a template; import cannot order those safely, so \
                             build both keys from plain attributes instead",
                            entity.name, key.template, key.attribute, source
                        ));
                    }
                }
            }

            // Recorded before the rule-target retain below: this is a
            // property of the model's templates, not of what survives.
            entity_key_shapes.push(
                keys.iter()
                    .map(|key| {
                        (
                            key.attribute.clone(),
                            key.template.clone(),
                            key.source_paths().cloned().collect(),
                        )
                    })
                    .collect(),
            );

            for key in &keys {
                if rule_targets.contains(key.attribute.as_str()) {
                    warnings.push(Notice::caution(format!(
                        "a rule targets key attribute '{}' directly, so on any item that rule \
                         matches it is not rebuilt from entity '{}' template '{}' and the \
                         rule's value is kept as written",
                        key.attribute, entity.name, key.template
                    )));
                }
            }

            for key in &keys {
                for rule in &rules {
                    let Some(target) = rule_target(rule) else {
                        continue;
                    };

                    if !key.sources().any(|source| source == target) {
                        continue;
                    }
                    let consequence = match rule.action {
                        // NULL is not a string, so the template cannot render
                        // at all and the key keeps the value it arrived with.
                        // That is the opposite of collapsing, and worse.
                        ValidatedAction::Null => format!(
                            "no item of that entity can rebuild {}, so every one of them keeps \
                             the key value it arrived with. Use fake or hash instead",
                            key.attribute
                        ),
                        ValidatedAction::Redact => format!(
                            "every item of that entity renders the same {} and overwrites the \
                             last, leaving fewer rows than the export. Use fake or hash instead",
                            key.attribute
                        ),
                        // Masking keeps the last few characters, so two
                        // different values often mask to the same thing.
                        ValidatedAction::Mask { .. } => format!(
                            "items of that entity whose '{target}' masks to the same text render \
                             the same {} and overwrite each other. Use fake or hash instead",
                            key.attribute
                        ),
                        ValidatedAction::Fake { .. } | ValidatedAction::Hash { .. } => continue,
                    };
                    warnings.push(Notice::caution(format!(
                        "a rule rewrites '{target}', which entity '{}' builds {} from via \
                         template '{}': {consequence}",
                        entity.name, key.attribute, key.template
                    )));
                }
            }

            entities.push(EntityKeys {
                name: entity.name.clone(),
                type_attribute: type_attribute(model, entity),
                keys,
                match_keys,
            });
        }

        // Two entities that build the same key attribute are asserting their
        // keys can agree, which is how a single-table design keeps a customer
        // and its orders in one partition. If a rule rewrites an attribute
        // one of those templates reads, each entity anonymises it
        // independently and the two can stop agreeing.
        //
        // Grouping on the key attribute alone, not on the template text, is
        // deliberate: the canonical join is `CUSTOMER#${id}` against
        // `CUSTOMER#${customerId}`, two different templates that produce the
        // same partition. Requiring identical text would miss it. Unrelated
        // entities that merely share a key name cost nothing here, because
        // what is actually compared later is the key *value* an item arrived
        // with, and `account#acc1` never equalled `project#p1`.
        //
        // Consistency-tracked roots are tracked too, and only excused from
        // the warning. The consistency map stops taking new values at its own
        // cap and starts handing out fresh ones, so a field listed in
        // [consistency] is not a permanent guarantee, and the value check is
        // what notices when it lapses.
        type SharedGroups = Vec<(String, (Vec<usize>, Vec<String>, Vec<usize>))>;
        let mut shared: SharedGroups = Vec::new();
        for (idx, shape) in entity_key_shapes.iter().enumerate() {
            for (attribute, _template, sources) in shape {
                // An entity whose sources no rule rewrites keeps its key as
                // it arrived. It is not at risk itself, but it is what a
                // rewritten sibling has to keep agreeing with, so it observes
                // the group rather than joining it.
                let roots: Vec<String> = sources
                    .iter()
                    .filter_map(|source| match source.first() {
                        Some(PathElement::Attribute(root)) => Some(root.clone()),
                        _ => None,
                    })
                    .filter(|root| rule_targets.contains(root.as_str()))
                    .collect();
                let group = match shared.iter_mut().find(|(k, _)| k == attribute) {
                    Some((_, group)) => group,
                    None => {
                        shared.push((attribute.clone(), (Vec::new(), Vec::new(), Vec::new())));
                        &mut shared.last_mut().expect("just pushed").1
                    }
                };
                let (users, known_roots, observers) = group;
                // A rule naming the key attribute itself rewrites the whole
                // key, which puts the entity at risk exactly as a rule on one
                // of the template's sources does. Reading only the sources put
                // it among the observers, and a group with no user at all is
                // dropped below, so the one shape that rewrites the most of a
                // key was the shape that went unchecked.
                let rewrites_key_directly = rule_targets.contains(attribute.as_str());
                if roots.is_empty() && !rewrites_key_directly {
                    if !observers.contains(&idx) {
                        observers.push(idx);
                    }
                    continue;
                }
                if !users.contains(&idx) {
                    users.push(idx);
                }
                for root in roots {
                    if !known_roots.contains(&root) {
                        known_roots.push(root);
                    }
                }
            }
        }
        for (_, (users, roots, observers)) in shared.iter_mut() {
            users.sort();
            roots.sort();
            observers.sort();
            observers.retain(|o| !users.contains(o));
        }
        // A group needs one entity a rule puts at risk. It does not need a
        // second entity to disagree with: an entity that takes one original
        // key to two values has already split the rows that shared it, and
        // requiring a sibling meant the commonest single-table shape, where
        // only one entity templates a key, was never checked at all.
        shared.retain(|(_, (users, _, _))| !users.is_empty());
        shared.sort_by(|a, b| a.0.cmp(&b.0));

        let at_risk: Vec<AtRisk> = shared
            .into_iter()
            .map(|(key_attribute, (entities_using, roots, observers))| {
                let unlisted: Vec<String> = roots
                    .iter()
                    .filter(|root| !consistency_fields.contains(*root))
                    .cloned()
                    .collect();
                // The up-front warning is for two entities a rule puts at
                // risk. An entity that only observes has nothing to list in
                // [consistency]; if its sibling diverges from it, the value
                // check reports that once it is seen.
                if !unlisted.is_empty() && entities_using.len() > 1 {
                    warnings.push(Notice::caution(format!(
                        "{} both build {} from an attribute outside [consistency] fields ({}): \
                         if they both appear in this import their keys may not agree and the \
                         entities will not join",
                        entity_list(&entities, &entities_using),
                        key_attribute,
                        quoted_list(&unlisted)
                    )));
                }
                AtRisk {
                    grouped_by: grouped_by
                        .get(key_attribute.as_str())
                        .map(|parts| parts.iter().map(|a| (*a).to_string()).collect())
                        .unwrap_or_default(),
                    key_attribute,
                    // Only the roots the rules file has NOT already listed.
                    // Falling back to every root told an operator to add
                    // fields they had added, which reads as the advice not
                    // working rather than as the map having lapsed.
                    consistency_roots: unlisted,
                    entities: entities_using,
                    observers,
                    seen_keys: std::collections::HashMap::new(),
                    diverged: HashSet::new(),
                    split_alone: HashSet::new(),
                    unchecked: 0,
                }
            })
            .collect();

        let mut type_attributes: Vec<String> =
            entities.iter().map(|e| e.type_attribute.clone()).collect();
        type_attributes.sort();
        type_attributes.dedup();

        Ok((
            Self {
                entities,
                type_attributes,
                hash_attribute: hash.map(String::from),
                range_attribute: range.map(String::from),
                warned_mismatch: HashMap::new(),
                rule_targets: rule_targets.iter().map(|a| (*a).to_string()).collect(),
                schema_keys,
                rule_paths,
                survived: HashMap::new(),
                warned_unrenderable: HashMap::new(),
                warned_rule_wins: HashSet::new(),
                warned_ambiguous: HashSet::new(),
                unmatched: 0,
                at_risk,
                rebuilt_keys: HashSet::new(),
                collisions: 0,
                collision_check_capped: false,
            },
            warnings,
        ))
    }

    /// Before the rules run: decide which of the item's keys will be rebuilt.
    ///
    /// A key qualifies when the entity's template reproduces the value the
    /// item arrived with. Any key the template does not reproduce is left
    /// alone and reported (once per entity and key). An item that matches no
    /// entity is counted for [`take_unmatched`](Self::take_unmatched).
    pub fn plan(&mut self, item: &Item, warnings: &mut Vec<Notice>) -> Option<Rederivation> {
        let (entity_idx, also) = match self.resolve_entity(item) {
            Some(found) => found,
            None => {
                self.unmatched += 1;
                return None;
            }
        };
        if !also.is_empty() {
            // Without a type attribute the model cannot say whose row this
            // is, and taking the first entity in model order picks one in
            // silence. The item is still rebuilt as that entity, so nothing
            // is left unrebuilt; but if the guess is wrong the keys are
            // rendered from the wrong templates, and that is worth a line.
            let mut group = vec![entity_idx];
            group.extend(also.iter().copied());
            if self.warned_ambiguous.insert(group.clone()) {
                warnings.push(Notice::caution(format!(
                    "{} all reproduce the keys of at least one item that carries no type \
                     attribute, so it was taken as entity '{}', the first in the model. Give \
                     items a type attribute if that is not the entity they belong to",
                    entity_list(&self.entities, &group),
                    self.entities[entity_idx].name
                )));
            }
        }

        let entity = &self.entities[entity_idx];

        let mut keys = Vec::new();
        let mut rewritten_directly: Vec<&str> = Vec::new();
        for (idx, key) in entity.keys.iter().enumerate() {
            let Some(current) = item.get(&key.attribute) else {
                // A sparse index key: nothing to rebuild.
                continue;
            };
            let reproduced = match current {
                AttributeValue::S(s) => render(&key.segments, item).as_deref() == Some(s.as_str()),
                _ => false,
            };
            if reproduced {
                keys.push(idx);
            } else if self.rule_targets.contains(&key.attribute) {
                // A rule names this key directly, so whatever its template
                // would have rendered is beside the point: the rule's value
                // replaces it. Counting it among the keys that kept their
                // original would claim real data survived a rule that removed
                // it. The "a rule rewrote {key} directly" warning covers it.
                // It still has a join to lose, though: two entities that
                // arrived sharing this key and leave it to an unseeded rule
                // split exactly as a rebuilt key would, so it is compared.
                rewritten_directly.push(key.attribute.as_str());
            } else {
                let seen = self.warned_mismatch.entry((entity_idx, idx)).or_insert(0);
                *seen += 1;
                if *seen == 1 {
                    warnings.push(Notice::caution(format!(
                        "entity '{}': template '{}' does not reproduce {} (the attributes it \
                         names are missing, not scalars, or the key was built differently); \
                         such keys are left unchanged, and the count is reported once the \
                         table has been read",
                        entity.name, key.template, key.attribute
                    )));
                }
            }
        }

        // Only a key that will actually change takes part in the join
        // check: one the template rebuilds, or one a rule rewrites outright.
        // One left holding its original value has already been reported as
        // a template that does not reproduce, and that is the accurate
        // diagnostic: comparing it against an entity that did rebuild would
        // call it a broken join and advise [consistency], which cannot fix
        // an attribute the item does not carry.
        let mut changing: Vec<&str> = keys
            .iter()
            .map(|idx| entity.keys[*idx].attribute.as_str())
            .collect();
        changing.extend(rewritten_directly);
        let at_risk_originals = self.at_risk_originals(entity_idx, item, &changing);

        // The value each applicable rule is about to replace, read through
        // the rule's full path so a rule on `profile.email` records the
        // address inside the map rather than nothing. Keyed by the top-level
        // attribute, which is what `apply_rules` reports as rewritten.
        // Bounded by the rules file rather than the item.
        let originals: Vec<(String, String)> = self
            .rule_paths
            .iter()
            .filter_map(|path| {
                let root = match path.first()? {
                    PathElement::Attribute(name) => name.clone(),
                    _ => return None,
                };
                match resolve_path(item, path)? {
                    AttributeValue::S(s) => Some((root, s)),
                    AttributeValue::N(n) => Some((root, n)),
                    _ => None,
                }
            })
            .collect();

        Some(Rederivation {
            entity: entity_idx,
            keys,
            originals,
            at_risk_originals,
        })
    }

    /// The key value this item arrived with, for each at-risk group whose key
    /// this item will actually rebuild.
    fn at_risk_originals(
        &self,
        entity_idx: usize,
        item: &Item,
        rebuilt: &[&str],
    ) -> Vec<(usize, u64)> {
        self.at_risk
            .iter()
            .enumerate()
            .filter(|(_, risk)| {
                (risk.entities.contains(&entity_idx) || risk.observers.contains(&entity_idx))
                    && rebuilt.iter().any(|a| *a == risk.key_attribute)
            })
            .flat_map(|(idx, risk)| {
                // A sort key needs its partition to say which rows it was
                // grouped with, and it may sort several indexes, each its own
                // grouping. An item not carrying one of those attributes
                // cannot be shown to have shared that group, so that grouping
                // is left out rather than compared against rows it may never
                // have sat beside.
                let Some(key) = item.get(&risk.key_attribute) else {
                    return Vec::new();
                };
                if risk.grouped_by.is_empty() {
                    return group_hash(None, key)
                        .map(|h| (idx, h))
                        .into_iter()
                        .collect();
                }
                risk.grouped_by
                    .iter()
                    .filter_map(|attribute| {
                        let value = item.get(attribute)?;
                        group_hash(Some((attribute.as_str(), value)), key).map(|h| (idx, h))
                    })
                    .collect()
            })
            .collect()
    }

    /// After the rules ran: compare what this item's at-risk values became
    /// against what an earlier item of a different entity made of the same
    /// original. Two entities that agree, because the action is
    /// deterministic or because no rule matched, are left alone.
    fn note_at_risk_results(&mut self, plan: &Rederivation, item: &Item) {
        for (risk_idx, original) in &plan.at_risk_originals {
            let risk = &mut self.at_risk[*risk_idx];
            let Some(now) = item.get(&risk.key_attribute).and_then(scalar_hash) else {
                continue;
            };

            // A key already recorded is still compared after the cap. Only a
            // key never seen before goes unchecked, and that is counted so
            // the run can say the check was partial rather than clean.
            if !risk.seen_keys.contains_key(original) {
                if risk.seen_keys.len() >= MAX_TRACKED_KEYS {
                    risk.unchecked += 1;
                    continue;
                }
                risk.seen_keys.insert(*original, Vec::new());
            }
            let groups_rows = risk.grouped_by.is_empty();
            let outcomes = risk.seen_keys.get_mut(original).expect("just inserted");

            match outcomes.iter_mut().find(|o| o.entity == plan.entity) {
                Some(mine) => mine.multiple |= mine.first != now,
                None => outcomes.push(Outcome {
                    entity: plan.entity,
                    first: now,
                    multiple: false,
                }),
            }

            // Two entities disagree when their results differ, or when either
            // produced more than one result, since one of those must differ
            // from what the other produced.
            let mut diverged: Vec<usize> = Vec::new();
            let mut split_alone: Vec<usize> = Vec::new();
            for (i, a) in outcomes.iter().enumerate() {
                // An entity that took one original key to more than one result
                // has split the rows that shared it, and the pairwise pass
                // below cannot see that because it never pairs an entity with
                // itself. What that split costs depends on the key. Losing a
                // partition means rows that came back from one query no longer
                // do, so it fails the import. Losing a shared sort value
                // inside a partition is weaker, and rows share one for
                // reasons that are not relationships: two people called John
                // Smith sat under one tenant share `NAME#John Smith` and were
                // never related. Failing on that blocks a legitimate export,
                // and the only way past would be to drop --data-model, which
                // turns key rebuilding off and ships the real values.
                if a.multiple {
                    if groups_rows {
                        diverged.push(a.entity);
                    } else {
                        split_alone.push(a.entity);
                    }
                }
                for b in outcomes.iter().skip(i + 1) {
                    if a.first != b.first || a.multiple || b.multiple {
                        diverged.push(a.entity);
                        diverged.push(b.entity);
                    }
                }
            }
            for entity in diverged {
                risk.diverged.insert(entity);
            }
            for entity in split_alone {
                risk.split_alone.insert(entity);
            }
        }
    }

    /// After the rules run: render every planned key from the item's current
    /// attributes. A key whose template no longer renders (a rule nulled or
    /// removed an attribute it needs) is left unchanged and reported.
    pub fn apply(
        &mut self,
        plan: &Rederivation,
        rewritten_by_rules: &HashSet<String>,
        item: &mut Item,
        warnings: &mut Vec<Notice>,
    ) {
        let entity = &self.entities[plan.entity];
        let mut rule_wins = Vec::new();
        for &idx in &plan.keys {
            let key = &entity.keys[idx];
            // A rule that actually rewrote this key wins over its template.
            // Taken from what the rules did rather than from what their
            // conditions predicted: each rule sees the item as the rules
            // before it left it, so a prediction made up front can be wrong.
            if rewritten_by_rules.contains(&key.attribute) {
                rule_wins.push(idx);
                continue;
            }
            match render(&key.segments, item) {
                Some(value) => {
                    item.insert(key.attribute.clone(), AttributeValue::S(value));
                }
                None => {
                    let seen = self
                        .warned_unrenderable
                        .entry((plan.entity, idx))
                        .or_insert(0);
                    *seen += 1;
                    if *seen == 1 {
                        warnings.push(Notice::caution(format!(
                            "entity '{}': cannot rebuild {} from template '{}' after the rules \
                             ran (an attribute it needs is no longer a string or number); \
                             the original key value is left in place, and the count is \
                             reported once the table has been read",
                            entity.name, key.attribute, key.template
                        )));
                    }
                }
            }
        }
        for idx in rule_wins {
            if self.warned_rule_wins.insert((plan.entity, idx)) {
                let key = &self.entities[plan.entity].keys[idx];
                warnings.push(Notice::caution(format!(
                    "entity '{}': a rule rewrote {} directly, so it was not rebuilt from \
                     template '{}'",
                    self.entities[plan.entity].name, key.attribute, key.template
                )));
            }
        }

        // A key the model gives this entity no template for is not rebuilt,
        // and nothing above looked at it. If a value a rule just replaced is
        // still sitting inside it, that is real data in a key on a run that
        // would otherwise report success. Judged on what is seen in the key,
        // not on the index existing.
        let survivals: Vec<String> = {
            let entity = &self.entities[plan.entity];
            self.schema_keys
                .iter()
                .filter(|key| !entity.match_keys.iter().any(|k| &k.attribute == *key))
                .filter(|key| match item.get(key.as_str()) {
                    Some(AttributeValue::S(value)) => plan.originals.iter().any(|(attr, old)| {
                        rewritten_by_rules.contains(attr) && holds_as_component(value, old)
                    }),
                    _ => false,
                })
                .cloned()
                .collect()
        };
        for key in survivals {
            let seen = self.survived.entry((plan.entity, key.clone())).or_insert(0);
            *seen += 1;
            if *seen == 1 {
                warnings.push(Notice::caution(format!(
                    "entity '{}': {key} still holds a value a rule replaced, and the model gives \
                     this entity no template for {key}, so nothing rebuilt it; the count is \
                     reported once the table has been read",
                    self.entities[plan.entity].name
                )));
            }
        }

        // Every item's primary key is recorded, not only a rebuilt one: a
        // rebuilt key can land on a row that was left alone, and counting
        // only rebuilds would miss the row that got overwritten.
        self.note_primary_key(item);
        self.note_at_risk_results(plan, item);
    }

    /// Everything this table's derivation has to say, once it has been read,
    /// each item with its concern decided here. The join break is the one
    /// outcome not in this list, because it is an error rather than a notice;
    /// see [`KeyDeriver::join_breaks`].
    ///
    /// Exposures are what the run saw: rows whose keys were never rebuilt,
    /// keys that still hold a value a rule replaced. Cautions are what it
    /// could not do or could not check: a split sort key, a cap reached, an
    /// overwrite. Nothing else in the importer decides which is which for
    /// these.
    pub fn take_notices(&mut self) -> Vec<Notice> {
        let mut out = Vec::new();

        let unmatched = self.take_unmatched();
        if unmatched > 0 {
            out.push(Notice::exposure(format!(
                "{unmatched} items matched no entity in the data model (no type attribute, \
                 and no entity's key templates reproduce their keys); their keys were not \
                 rebuilt"
            )));
        }
        for (entity, key, count) in self.take_key_survivals() {
            out.push(Notice::exposure(format!(
                "entity '{entity}' carries {key} on {count} items with a value a rule \
                 replaced still inside it. The model gives that entity no template for \
                 {key}, so nothing rebuilt it"
            )));
        }
        for (entity, key, count) in self.take_unrebuilt() {
            out.push(Notice::exposure(format!(
                "entity '{entity}' kept the original {key} on {count} items, because its \
                 template could not rebuild them. Those keys still hold the values the \
                 export arrived with"
            )));
        }

        for message in self.sort_key_splits() {
            out.push(Notice::caution(message));
        }
        let unchecked = self.unchecked_join_keys();
        if unchecked > 0 {
            out.push(Notice::caution(format!(
                "the join check stopped taking on new keys once a group reached {}, so \
                 {unchecked} key comparisons were skipped; a broken join among those would \
                 not have been caught. Keys seen before the cap were still compared",
                MAX_TRACKED_KEYS
            )));
        }
        let (collisions, capped) = self.take_collisions();
        if collisions > 0 {
            out.push(Notice::caution(format!(
                "{collisions} items rendered the same primary key as an earlier item and \
                 overwrote it, so the output holds fewer rows than the export. Either a rule \
                 is replacing an attribute a key is built from with a constant, or a fake \
                 generator is drawing the same value twice: every generator except \
                 safe_email draws from a pool small enough that repeats are ordinary at a \
                 few hundred items, so prefer hash or a seeded safe_email for an attribute \
                 a key is built from"
            )));
        }
        if capped {
            out.push(Notice::caution(format!(
                "the rebuilt-key collision check stopped after {} keys; later collisions in \
                 this table were not counted",
                MAX_TRACKED_KEYS
            )));
        }
        out
    }

    /// Items whose untemplated key still held a value a rule replaced, by
    /// entity and key. These are values the run exists to remove.
    fn take_key_survivals(&mut self) -> Vec<(String, String, usize)> {
        let mut out: Vec<(String, String, usize)> = self
            .survived
            .drain()
            .map(|((entity, key), count)| (self.entities[entity].name.clone(), key, count))
            .collect();
        out.sort();
        out
    }

    /// Remember this item's primary key so a later item landing on the same
    /// one is counted as a collision. `apply` does this for every item it
    /// rebuilds; the pipeline calls it directly for an item that matched no
    /// entity, whose keys are kept as they arrived.
    pub(super) fn note_primary_key(&mut self, item: &Item) {
        if self.collision_check_capped {
            return;
        }
        if self.rebuilt_keys.len() >= MAX_TRACKED_KEYS {
            self.collision_check_capped = true;
            return;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for attribute in [&self.hash_attribute, &self.range_attribute]
            .into_iter()
            .flatten()
        {
            // Type tag as well as value: a numeric key is as much part of the
            // identity as a string one, and leaving it out reports two
            // distinct rows as an overwrite.
            match item.get(attribute) {
                Some(AttributeValue::S(s)) => {
                    1u8.hash(&mut hasher);
                    s.hash(&mut hasher);
                }
                Some(AttributeValue::N(n)) => {
                    2u8.hash(&mut hasher);
                    // The form the engine stores, where DynamoDB would accept
                    // the number. `1` and `1.0` land on one row, and counting
                    // them as two hid the overwrite this exists to count.
                    match crate::types::validate_dynamo_number(n) {
                        Ok(()) => crate::types::normalize_dynamo_number(n).hash(&mut hasher),
                        Err(_) => n.hash(&mut hasher),
                    }
                }
                Some(AttributeValue::B(b)) => {
                    3u8.hash(&mut hasher);
                    b.hash(&mut hasher);
                }
                _ => 0u8.hash(&mut hasher),
            }
        }
        if !self.rebuilt_keys.insert(hasher.finish()) {
            self.collisions += 1;
        }
    }

    /// Attributes whose entities have now both turned up in the data, so the
    /// join between them is actually broken rather than merely at risk.
    ///
    /// Checked once the items have been read rather than up front, and only
    /// for a value two entities actually share. The model says the two
    /// entities *can* collide; an import holding only one of them, or holding
    /// both with no value in common, has no join to lose, and failing there
    /// would earn a bypass flag.
    pub fn join_breaks(&self) -> Vec<String> {
        self.at_risk
            .iter()
            .filter(|risk| !risk.diverged.is_empty())
            .map(|risk| {
                let mut seen: Vec<usize> = risk.diverged.iter().copied().collect();
                seen.sort();

                // A rule naming the key attribute itself reads no template
                // source, so there is no root to list and [consistency] has
                // nothing to hold. Saying "add  to [consistency] fields" sends
                // the reader looking for a field name that was never there.
                let repair = if risk.consistency_roots.is_empty() {
                    "Use a deterministic action such as hash, so one original key always \
                     renders one new key"
                        .to_string()
                } else {
                    format!(
                        "Add {} to [consistency] fields, or use a deterministic action such \
                         as hash",
                        quoted_list(&risk.consistency_roots)
                    )
                };

                if seen.len() == 1 {
                    return format!(
                        "{} took one original {} to more than one value, so rows that shared a \
                         partition no longer share one and a query for them comes back with \
                         part of what it held. {repair}",
                        entity_list(&self.entities, &seen),
                        risk.key_attribute,
                    );
                }

                format!(
                    "{} took the same original {} to different values, so their keys no longer \
                     agree and they will not join. Give both entities the same attribute name \
                     for that value. {repair}",
                    entity_list(&self.entities, &seen),
                    risk.key_attribute,
                )
            })
            .collect()
    }

    /// Entities that split a sort key of their own. Not a broken join on its
    /// own, because rows share a sort value for reasons that are not
    /// relationships, but worth saying: a query matching that value used to
    /// return them together and no longer does.
    fn sort_key_splits(&mut self) -> Vec<String> {
        let entities = &self.entities;
        let mut out: Vec<String> = self
            .at_risk
            .iter_mut()
            .filter(|risk| !risk.split_alone.is_empty())
            .map(|risk| {
                // Taken, not read: a second call reports nothing.
                let mut seen: Vec<usize> = risk.split_alone.drain().collect();
                seen.sort();
                format!(
                    "{} took one original {} to more than one value. Those rows stay in their \
                     partitions, so nothing is lost from a query on the partition, but a query \
                     matching that {} exactly used to return them together and will not now",
                    entity_list(entities, &seen),
                    risk.key_attribute,
                    risk.key_attribute
                )
            })
            .collect();
        out.sort();
        out
    }

    /// How many items kept a key the model could not rebuild, by entity and
    /// key. These are the values the run exists to remove, so the count is the
    /// difference between one stray row and a whole table left as it arrived.
    fn take_unrebuilt(&mut self) -> Vec<(String, String, usize)> {
        // Both maps describe one outcome, a key left holding the value it
        // arrived with, so an entity and key in both is one fact with two
        // causes rather than two facts. Summed, not listed twice.
        let mut totals: HashMap<(usize, usize), usize> = HashMap::new();
        for (id, count) in self
            .warned_mismatch
            .iter()
            .chain(self.warned_unrenderable.iter())
        {
            *totals.entry(*id).or_insert(0) += count;
        }
        let mut out: Vec<(String, String, usize)> = totals
            .into_iter()
            .map(|((entity, key), count)| {
                let entity = &self.entities[entity];
                (
                    entity.name.clone(),
                    entity.keys[key].attribute.clone(),
                    count,
                )
            })
            .collect();
        out.sort();
        self.warned_mismatch.clear();
        self.warned_unrenderable.clear();
        out
    }

    /// Keys the join check could not take on after its cap, so the caller
    /// can say the check was partial rather than clean.
    fn unchecked_join_keys(&mut self) -> usize {
        self.at_risk
            .iter_mut()
            .map(|risk| std::mem::take(&mut risk.unchecked))
            .sum()
    }

    /// Number of items since the last call that matched no entity.
    fn take_unmatched(&mut self) -> usize {
        std::mem::take(&mut self.unmatched)
    }

    /// Rebuilt items whose primary key repeated an earlier rebuilt key, and
    /// whether the check stopped early because the table was too large to
    /// track in full.
    fn take_collisions(&mut self) -> (usize, bool) {
        (
            std::mem::take(&mut self.collisions),
            self.collision_check_capped,
        )
    }

    /// Find the item's entity: by its type attribute first, otherwise the
    /// first entity whose templates reproduce every key the item carries.
    ///
    /// Returns the entity and, when the item carried no type attribute and
    /// more than one entity's templates reproduce its keys, the others that
    /// did too. A type attribute settles it; a template match is a guess.
    fn resolve_entity(&self, item: &Item) -> Option<(usize, Vec<usize>)> {
        for type_attribute in &self.type_attributes {
            let Some(AttributeValue::S(type_value)) = item.get(type_attribute) else {
                continue;
            };
            let by_type = self
                .entities
                .iter()
                .position(|e| e.name == *type_value && e.type_attribute == *type_attribute);
            if let Some(idx) = by_type {
                return Some((idx, Vec::new()));
            }
        }

        let mut matches = self.entities.iter().enumerate().filter_map(|(idx, e)| {
            let mut present = 0;
            let all_match = e
                .match_keys
                .iter()
                .all(|key| match item.get(&key.attribute) {
                    None => true,
                    Some(AttributeValue::S(s)) => {
                        present += 1;
                        render(&key.segments, item).as_deref() == Some(s.as_str())
                    }
                    Some(_) => false,
                });
            (all_match && present > 0).then_some(idx)
        });
        let first = matches.next()?;
        Some((first, matches.collect()))
    }
}

/// `'a'`, or `'a' and 'b'`, for a message.
fn quoted_list(names: &[String]) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    match quoted.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => String::new(),
    }
}

/// Hash a scalar attribute value, or `None` for anything a key cannot hold.
/// Whether `key` carries `value` as a whole `#`-separated component.
///
/// Keys in this style are built from components joined with `#`, so a value
/// a rule replaced shows up in one as a component: `CUSTOMER#alice@x` holds
/// `alice@x`. Matching anywhere in the string instead would report
/// `ORDER#1042` as holding a replaced `42`, and an exposure that fires on a
/// coincidence is one people learn to wave through.
fn holds_as_component(key: &str, value: &str) -> bool {
    !value.is_empty() && (key == value || key.split('#').any(|component| component == value))
}

/// Which rows a key value is grouped with, as one hash.
///
/// For a partition key that is the value itself. For a sort key it is the
/// partition value and the sort value together, because two rows only share a
/// group when they share both: `pk` and `sk` are unique together on a base
/// table, so rows sharing a sort value there are in different partitions and
/// have no join to lose, while a GSI sort key is not unique and rows really
/// can share one inside a partition.
fn group_hash(grouping: Option<(&str, &AttributeValue)>, key: &AttributeValue) -> Option<u64> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    if let Some((attribute, value)) = grouping {
        // The attribute name, not only its value. Two indexes can hold the
        // same partition string for unrelated reasons, and absorbing the value
        // alone put them in one bucket, where a split in either read as a
        // disagreement across both.
        attribute.hash(&mut hasher);
        scalar_hash(value)?.hash(&mut hasher);
    }
    scalar_hash(key)?.hash(&mut hasher);
    Some(hasher.finish())
}

fn scalar_hash(value: &AttributeValue) -> Option<u64> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match value {
        AttributeValue::S(s) => s.hash(&mut hasher),
        // Numbers are hashed on the form the engine stores, so a key written
        // `1` and a key written `1.0` are one key here as they are there.
        // Only where DynamoDB would accept the number: normalising maps
        // anything it cannot read to "0", which would put unrelated keys in
        // one bucket.
        AttributeValue::N(n) => match crate::types::validate_dynamo_number(n) {
            Ok(()) => crate::types::normalize_dynamo_number(n).hash(&mut hasher),
            Err(_) => n.hash(&mut hasher),
        },
        // DynamoDB allows a binary key. Returning None dropped those items
        // from the check entirely, so a genuine split inside a binary
        // partition shipped with nothing said.
        AttributeValue::B(b) => b.hash(&mut hasher),
        _ => return None,
    }
    Some(hasher.finish())
}

/// "entity 'A' and entity 'B'", for a warning.
fn entity_list(entities: &[EntityKeys], indices: &[usize]) -> String {
    let names: Vec<String> = indices
        .iter()
        .map(|i| format!("entity '{}'", entities[*i].name))
        .collect();
    match names.split_last() {
        Some((last, [])) => last.clone(),
        Some((last, rest)) => format!("{} and {last}", rest.join(", ")),
        None => String::new(),
    }
}

/// The top-level attribute a rule rewrites.
fn rule_target(rule: &ValidatedRule) -> Option<&str> {
    match rule.path.first() {
        Some(PathElement::Attribute(name)) => Some(name.as_str()),
        _ => None,
    }
}

/// Record `attribute`'s template. Constant templates are kept too: they do
/// not need rebuilding, but they are often the only thing telling two
/// entities apart when an item carries no type attribute.
fn push_key(
    keys: &mut Vec<TemplatedKey>,
    entity: &EntityDefinition,
    attribute: Option<&str>,
    template: Option<&str>,
) -> Result<(), String> {
    let (Some(attribute), Some(template)) = (attribute, template) else {
        return Ok(());
    };
    if template.is_empty() {
        // No template at all (a plain attribute key). Nothing to rebuild, and
        // nothing that could tell one entity from another.
        return Ok(());
    }
    let segments = parse_template(template)
        .map_err(|e| format!("entity '{}', {attribute}: {e}", entity.name))?;
    keys.push(TemplatedKey {
        attribute: attribute.to_string(),
        template: template.to_string(),
        segments,
    });
    Ok(())
}

fn type_attribute(model: &DataModel, entity: &EntityDefinition) -> String {
    entity
        .type_attribute
        .clone()
        .unwrap_or_else(|| model.type_attribute.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::condition;
    use crate::schema::GsiMapping;
    use crate::types::{KeySchemaElement, KeyType};

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), AttributeValue::S(v.to_string())))
            .collect()
    }

    fn entity(
        name: &str,
        pk: &str,
        sk: Option<&str>,
        gsi: Option<(&str, Option<&str>)>,
    ) -> EntityDefinition {
        EntityDefinition {
            name: name.to_string(),
            pk_template: pk.to_string(),
            sk_template: sk.map(String::from),
            type_attribute: None,
            gsi_mappings: gsi
                .map(|(pk, sk)| {
                    vec![GsiMapping {
                        index_name: "GSI1".to_string(),
                        pk_template: pk.to_string(),
                        sk_template: sk.map(String::from),
                    }]
                })
                .unwrap_or_default(),
            description: None,
        }
    }

    fn model() -> DataModel {
        DataModel {
            schema_format: "onetable:1.1.0".to_string(),
            type_attribute: "_type".to_string(),
            entities: vec![
                entity("Account", "account#${id}", Some("account#"), None),
                entity(
                    "User",
                    "account#${accountId}",
                    Some("user#${email}"),
                    Some(("user#${email}", Some("user#"))),
                ),
            ],
        }
    }

    fn model_with(entities: Vec<EntityDefinition>) -> DataModel {
        DataModel {
            schema_format: "onetable:1.1.0".to_string(),
            type_attribute: "_type".to_string(),
            entities,
        }
    }

    fn request() -> CreateTableRequest {
        serde_json::from_value(serde_json::json!({
            "TableName": "App",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gs1pk", "AttributeType": "S"},
                {"AttributeName": "gs1sk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "gs1pk", "KeyType": "HASH"},
                    {"AttributeName": "gs1sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap()
    }

    fn two_index_request() -> CreateTableRequest {
        serde_json::from_value(serde_json::json!({
            "TableName": "App",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gs1pk", "AttributeType": "S"},
                {"AttributeName": "gs1sk", "AttributeType": "S"},
                {"AttributeName": "gs2pk", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "gs1pk", "KeyType": "HASH"},
                        {"AttributeName": "gs1sk", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                },
                {
                    "IndexName": "GSI2",
                    "KeySchema": [
                        {"AttributeName": "gs2pk", "KeyType": "HASH"},
                        {"AttributeName": "gs1sk", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "ALL"}
                }
            ]
        }))
        .unwrap()
    }

    fn key_schema(name: &str, kind: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type: kind,
        }
    }

    fn rule(path: &str, action: ValidatedAction) -> ValidatedRule {
        ValidatedRule {
            tables: None,
            condition: condition::parse("attribute_exists(pk)").unwrap(),
            names: None,
            values: None,
            path: parse_path(path).unwrap(),
            action,
        }
    }

    fn no_consistency() -> HashSet<String> {
        HashSet::new()
    }

    fn no_rewrites() -> HashSet<String> {
        HashSet::new()
    }

    fn deriver() -> KeyDeriver {
        let (deriver, warnings) =
            KeyDeriver::new(&model(), &request(), &[], &no_consistency()).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        deriver
    }

    fn tracked(deriver: &KeyDeriver, entity: usize) -> Vec<&str> {
        deriver.entities[entity]
            .keys
            .iter()
            .map(|k| k.attribute.as_str())
            .collect()
    }

    fn user() -> Item {
        item(&[
            ("_type", "User"),
            ("pk", "account#acc1"),
            ("sk", "user#alice@example.com"),
            ("gs1pk", "user#alice@example.com"),
            ("gs1sk", "user#"),
            ("accountId", "acc1"),
            ("email", "alice@example.com"),
        ])
    }

    #[test]
    fn template_splits_into_literals_and_variables() {
        let var = |p: &str| Segment::Var {
            path: parse_path(p).unwrap(),
            pad: None,
        };
        assert_eq!(
            parse_template("project#${status}#${name}").unwrap(),
            vec![
                Segment::Literal("project#".to_string()),
                var("status"),
                Segment::Literal("#".to_string()),
                var("name"),
            ]
        );
        assert_eq!(
            parse_template("account#").unwrap(),
            vec![Segment::Literal("account#".to_string())]
        );
        assert_eq!(parse_template("${id}").unwrap(), vec![var("id")]);
        assert_eq!(
            parse_template("city#${address.city}").unwrap(),
            vec![Segment::Literal("city#".to_string()), var("address.city")]
        );
    }

    #[test]
    fn template_rejects_an_unclosed_reference() {
        let err = parse_template("user#${email").unwrap_err();
        assert!(err.contains("unclosed"), "{err}");

        let err = KeyDeriver::new(
            &model_with(vec![entity("Bad", "x#${id", None, None)]),
            &request(),
            &[],
            &no_consistency(),
        )
        .unwrap_err();
        assert!(err.contains("entity 'Bad', pk"), "{err}");
    }

    #[test]
    fn template_pads_a_value_the_way_onetable_does() {
        // ${name:length:pad}, pad defaulting to "0", prefixed until length
        let segments = parse_template("order#${orderNo:5}").unwrap();
        assert_eq!(
            render(&segments, &item(&[("orderNo", "42")])).as_deref(),
            Some("order#00042")
        );

        let segments = parse_template("order#${orderNo:4:x}").unwrap();
        assert_eq!(
            render(&segments, &item(&[("orderNo", "42")])).as_deref(),
            Some("order#xx42")
        );

        // already at or over the length is left alone
        let segments = parse_template("order#${orderNo:2}").unwrap();
        assert_eq!(
            render(&segments, &item(&[("orderNo", "12345")])).as_deref(),
            Some("order#12345")
        );

        // padding applies to a number attribute too
        let mut numeric = Item::new();
        numeric.insert("n".to_string(), AttributeValue::N("7".to_string()));
        let segments = parse_template("${n:3}").unwrap();
        assert_eq!(render(&segments, &numeric).as_deref(), Some("007"));

        let err = parse_template("order#${orderNo:wide}").unwrap_err();
        assert!(err.contains("is not a length"), "{err}");
    }

    #[test]
    fn a_padded_key_rebuilds_from_its_anonymised_source() {
        let model = model_with(vec![entity(
            "Order",
            "order#${customerId}",
            Some("order#${orderNo:5}"),
            None,
        )]);
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &[], &no_consistency()).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");

        let mut item_warnings = Vec::new();
        let mut order = item(&[
            ("_type", "Order"),
            ("pk", "order#cust1"),
            ("sk", "order#00042"),
            ("customerId", "cust1"),
            ("orderNo", "42"),
        ]);
        let plan = d.plan(&order, &mut item_warnings).unwrap();
        assert!(item_warnings.is_empty(), "{item_warnings:?}");
        assert_eq!(plan.keys.len(), 2);

        order.insert("orderNo".to_string(), AttributeValue::S("7".to_string()));
        d.apply(&plan, &no_rewrites(), &mut order, &mut item_warnings);
        assert_eq!(order["sk"], AttributeValue::S("order#00007".to_string()));
    }

    #[test]
    fn render_uses_scalars_and_fails_on_anything_else() {
        let segments = parse_template("user#${email}").unwrap();
        assert_eq!(
            render(&segments, &item(&[("email", "a@b.c")])).as_deref(),
            Some("user#a@b.c")
        );
        assert_eq!(render(&segments, &item(&[])), None);

        let mut nulled = Item::new();
        nulled.insert("email".to_string(), AttributeValue::NULL(true));
        assert_eq!(render(&segments, &nulled), None);
    }

    #[test]
    fn render_reaches_into_nested_maps() {
        let segments = parse_template("city#${address.city}").unwrap();
        let mut address = std::collections::HashMap::new();
        address.insert("city".to_string(), AttributeValue::S("Leeds".to_string()));
        let mut it = Item::new();
        it.insert("address".to_string(), AttributeValue::M(address));
        assert_eq!(render(&segments, &it).as_deref(), Some("city#Leeds"));
    }

    #[test]
    fn templated_keys_resolve_against_the_table_key_schema() {
        let d = deriver();
        // gs1sk is "user#" with no variable, so it is not tracked
        assert_eq!(tracked(&d, 1), vec!["pk", "sk", "gs1pk"]);
        assert_eq!(tracked(&d, 0), vec!["pk"]);
    }

    #[test]
    fn gsi_mapping_without_a_matching_index_is_skipped() {
        let mut request = request();
        request.global_secondary_indexes = None;
        let (d, _) = KeyDeriver::new(&model(), &request, &[], &no_consistency()).unwrap();
        assert_eq!(tracked(&d, 1), vec!["pk", "sk"]);
    }

    #[test]
    fn rebuilds_keys_from_the_anonymised_attribute() {
        let mut d = deriver();
        let mut warnings = Vec::new();
        let mut user = user();

        let plan = d.plan(&user, &mut warnings).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(plan.keys.len(), 3);

        user.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);

        assert_eq!(user["pk"], AttributeValue::S("account#acc1".to_string()));
        assert_eq!(
            user["sk"],
            AttributeValue::S("user#fake@example.org".to_string())
        );
        assert_eq!(
            user["gs1pk"],
            AttributeValue::S("user#fake@example.org".to_string())
        );
        assert_eq!(user["gs1sk"], AttributeValue::S("user#".to_string()));
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(d.take_collisions(), (0, false));
    }

    #[test]
    fn a_key_the_template_does_not_reproduce_is_left_and_reported_once_without_its_value() {
        let mut d = deriver();
        let mut warnings = Vec::new();

        for n in 0..3 {
            let mut user = item(&[
                ("_type", "User"),
                ("pk", "account#acc1"),
                ("sk", "legacy-profile"),
                ("accountId", "acc1"),
                ("email", "alice@example.com"),
            ]);
            let plan = d.plan(&user, &mut warnings).unwrap();
            assert_eq!(plan.keys.len(), 1, "only pk reproduces");

            user.insert(
                "email".to_string(),
                AttributeValue::S(format!("fake{n}@example.org")),
            );
            d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);
            assert_eq!(user["sk"], AttributeValue::S("legacy-profile".to_string()));
        }

        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("entity 'User'"));
        assert!(warnings[0].contains("does not reproduce sk"));
        assert!(
            !warnings[0].contains("legacy-profile"),
            "a warning must not quote the key value: {}",
            warnings[0]
        );
    }

    #[test]
    fn a_sparse_index_key_is_not_a_mismatch() {
        let mut d = deriver();
        let mut warnings = Vec::new();
        let mut user = user();
        user.remove("gs1pk");
        user.remove("gs1sk");
        let plan = d.plan(&user, &mut warnings).unwrap();
        assert_eq!(plan.keys.len(), 2);
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn a_key_that_stops_rendering_after_the_rules_is_reported_even_after_a_mismatch() {
        let mut d = deriver();
        let mut warnings = Vec::new();

        // First item: sk off-template, so the mismatch warning fires for (User, sk).
        let legacy = item(&[
            ("_type", "User"),
            ("pk", "account#acc1"),
            ("sk", "legacy-profile"),
            ("accountId", "acc1"),
            ("email", "alice@example.com"),
        ]);
        d.plan(&legacy, &mut warnings).unwrap();
        assert_eq!(warnings.len(), 1);

        // Second item: sk on-template, but the rule nulls email so it cannot render.
        let mut user = user();
        let plan = d.plan(&user, &mut warnings).unwrap();
        user.insert("email".to_string(), AttributeValue::NULL(true));
        d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);

        assert_eq!(
            user["sk"],
            AttributeValue::S("user#alice@example.com".to_string())
        );
        // sk and gs1pk both build from email, so both report
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings[1].contains("cannot rebuild sk"), "{warnings:?}");
        assert!(warnings[2].contains("cannot rebuild gs1pk"), "{warnings:?}");
    }

    #[test]
    fn entity_falls_back_to_template_shape_without_a_type_attribute() {
        let mut d = deriver();
        let mut warnings = Vec::new();

        let user = item(&[
            ("pk", "account#acc1"),
            ("sk", "user#alice@example.com"),
            ("accountId", "acc1"),
            ("email", "alice@example.com"),
        ]);
        let plan = d.plan(&user, &mut warnings).unwrap();
        assert_eq!(d.entities[plan.entity].name, "User");

        let account = item(&[("pk", "account#acc1"), ("sk", "account#"), ("id", "acc1")]);
        let plan = d.plan(&account, &mut warnings).unwrap();
        assert_eq!(d.entities[plan.entity].name, "Account");

        let stranger = item(&[("pk", "thing#1"), ("sk", "meta")]);
        assert!(d.plan(&stranger, &mut warnings).is_none());
        assert_eq!(d.take_unmatched(), 1);
        assert_eq!(d.take_unmatched(), 0);
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn primary_key_names_come_from_the_key_schema_not_the_model() {
        let mut request = request();
        request.key_schema = vec![
            key_schema("PK", KeyType::HASH),
            key_schema("SK", KeyType::RANGE),
        ];
        let (d, _) = KeyDeriver::new(&model(), &request, &[], &no_consistency()).unwrap();
        assert_eq!(tracked(&d, 1), vec!["PK", "SK", "gs1pk"]);
    }

    #[test]
    fn a_rule_on_a_key_attribute_wins_on_the_items_it_matches() {
        let rules = [rule("sk", ValidatedAction::Redact)];
        let (mut d, warnings) =
            KeyDeriver::new(&model(), &request(), &rules, &no_consistency()).unwrap();
        // The key stays tracked: whether the rule wins is an per-item question.
        assert_eq!(tracked(&d, 1), vec!["pk", "sk", "gs1pk"]);
        // Both entities are warned about, including the one whose sk template
        // is a constant. A rule naming the attribute overwrites that constant
        // just as it overwrites a rendered one, so the key stops saying what
        // the model says it says.
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(
            warnings
                .iter()
                .all(|w| w.contains("targets key attribute 'sk' directly")),
            "{warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("'Account'")),
            "the constant template must be named too: {warnings:?}"
        );

        let mut item_warnings = Vec::new();
        let mut user = user();
        let plan = d.plan(&user, &mut item_warnings).unwrap();
        // The rules rewrote sk, so the deriver must leave it alone.
        user.insert(
            "sk".to_string(),
            AttributeValue::S("[REDACTED]".to_string()),
        );
        let rewritten: HashSet<String> = ["sk".to_string()].into_iter().collect();
        d.apply(&plan, &rewritten, &mut user, &mut item_warnings);
        assert_eq!(user["sk"], AttributeValue::S("[REDACTED]".to_string()));
        assert!(
            item_warnings
                .iter()
                .any(|w| w.contains("a rule rewrote sk")),
            "{item_warnings:?}"
        );
    }

    #[test]
    fn a_rule_on_a_key_that_does_not_match_this_item_leaves_the_rebuild_alone() {
        // The rule targets sk but only matches Account items. A User's sk
        // must still be rebuilt, or the real email survives in the key.
        let rules = [
            ValidatedRule {
                tables: None,
                condition: condition::parse("attribute_exists(accountName)").unwrap(),
                names: None,
                values: None,
                path: parse_path("sk").unwrap(),
                action: ValidatedAction::Redact,
            },
            rule(
                "email",
                ValidatedAction::Fake {
                    generator: "safe_email".into(),
                    seed: None,
                },
            ),
        ];
        let (mut d, _) = KeyDeriver::new(&model(), &request(), &rules, &no_consistency()).unwrap();

        let mut warnings = Vec::new();
        let mut user = user();
        assert!(!user.contains_key("accountName"), "the rule must not match");
        let plan = d.plan(&user, &mut warnings).unwrap();

        user.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);

        assert_eq!(
            user["sk"],
            AttributeValue::S("user#fake@example.org".to_string()),
            "sk must be rebuilt: the sk rule never matched this item"
        );
    }

    #[test]
    fn a_constant_action_on_a_key_source_is_reported_up_front() {
        let rules = [rule("email", ValidatedAction::Redact)];
        let (_, warnings) =
            KeyDeriver::new(&model(), &request(), &rules, &no_consistency()).unwrap();
        // sk and gs1pk of User both build from email
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[0].contains("overwrites the last"), "{warnings:?}");
        assert!(warnings[0].contains("'email'"));

        let rules = [rule(
            "email",
            ValidatedAction::Fake {
                generator: "safe_email".into(),
                seed: None,
            },
        )];
        let (_, warnings) =
            KeyDeriver::new(&model(), &request(), &rules, &no_consistency()).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    /// Two entities keyed on the same attribute, as a single-table design
    /// puts a customer and its orders in one partition.
    fn shared_key_model() -> DataModel {
        model_with(vec![
            entity("Customer", "CUSTOMER#${email}", Some("PROFILE"), None),
            entity("Order", "CUSTOMER#${email}", Some("ORDER#${orderId}"), None),
        ])
    }

    /// The check only fires for an attribute a rule actually rewrites.
    fn email_rule() -> [ValidatedRule; 1] {
        [rule(
            "email",
            ValidatedAction::Fake {
                generator: "safe_email".into(),
                seed: None,
            },
        )]
    }

    #[test]
    fn a_shared_key_attribute_outside_consistency_warns_up_front() {
        let (deriver, warnings) = KeyDeriver::new(
            &shared_key_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("entity 'Customer' and entity 'Order'"));
        assert!(warnings[0].contains("'email'"));
        assert!(warnings[0].contains("will not join"));
        // Nothing seen yet, so nothing is broken yet.
        assert!(deriver.join_breaks().is_empty());
    }

    #[test]
    fn listing_the_attribute_in_consistency_silences_it() {
        let consistency: HashSet<String> = ["email".to_string()].into_iter().collect();
        let (_, warnings) =
            KeyDeriver::new(&shared_key_model(), &request(), &email_rule(), &consistency).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn an_attribute_no_rule_rewrites_is_not_at_risk() {
        // Same shared template, but nothing anonymises email, so the two
        // keys still agree in the output.
        let (_, warnings) =
            KeyDeriver::new(&shared_key_model(), &request(), &[], &no_consistency()).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn entities_reusing_an_attribute_name_on_different_keys_are_not_at_risk() {
        // Account keys pk on its own id, Task keys sk on its own id. Same
        // attribute name, different keys and templates, no join between them.
        let model = model_with(vec![
            entity("Account", "account#${id}", Some("account#"), None),
            entity("Task", "task#${projectId}", Some("task#${id}"), None),
        ]);
        let rules = [rule(
            "id",
            ValidatedAction::Fake {
                generator: "word".into(),
                seed: None,
            },
        )];
        let (_, warnings) = KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn one_entity_alone_is_not_a_broken_join() {
        let (mut d, _) = KeyDeriver::new(
            &shared_key_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();
        let mut warnings = Vec::new();

        for n in 0..3 {
            let customer = item(&[
                ("_type", "Customer"),
                ("pk", &format!("CUSTOMER#c{n}@x.co")),
                ("sk", "PROFILE"),
                ("email", &format!("c{n}@x.co")),
            ]);
            d.plan(&customer, &mut warnings).unwrap();
        }

        assert!(
            d.join_breaks().is_empty(),
            "a slice holding one entity has no join to lose: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn a_constant_key_template_still_tells_two_entities_apart() {
        // Customer and Order share a partition template and differ only by a
        // constant sk. With no type attribute to go on, matching that ignored
        // constant templates would resolve an Order as a Customer and leave
        // the Order's sk holding whatever it arrived with.
        let (mut d, _) = KeyDeriver::new(
            &shared_key_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();
        let mut warnings = Vec::new();

        let mut order = item(&[
            ("pk", "CUSTOMER#a@x.co"),
            ("sk", "ORDER#ref-a@x.co"),
            ("email", "a@x.co"),
            ("orderId", "ref-a@x.co"),
        ]);
        assert!(
            !order.contains_key("_type"),
            "no discriminator to fall back on"
        );

        let plan = d.plan(&order, &mut warnings).unwrap();
        assert_eq!(d.entities[plan.entity].name, "Order");

        order.insert("orderId".to_string(), AttributeValue::S("anon".to_string()));
        d.apply(&plan, &no_rewrites(), &mut order, &mut warnings);
        assert_eq!(
            order["sk"],
            AttributeValue::S("ORDER#anon".to_string()),
            "the Order's own sk must be rebuilt"
        );
    }

    #[test]
    fn a_key_built_from_another_templated_key_is_refused() {
        // pk reads sk, and sk is itself templated. Rendering pk first copies
        // the pre-anonymisation sk, so refuse rather than order by luck.
        let model = model_with(vec![entity("User", "${sk}", Some("user#${email}"), None)]);
        let err =
            KeyDeriver::new(&model, &request(), &email_rule(), &no_consistency()).unwrap_err();
        assert!(err.contains("reads key attribute 'sk'"), "{err}");
        assert!(err.contains("itself built from a template"), "{err}");

        // A key reading a plain attribute is fine.
        let model = model_with(vec![entity("User", "user#${email}", Some("profile"), None)]);
        assert!(KeyDeriver::new(&model, &request(), &email_rule(), &no_consistency()).is_ok());
    }

    #[test]
    fn a_gsi_sort_key_is_rebuilt_when_its_hash_key_is_a_plain_attribute() {
        // GSI1 hashes on a plain tenantId and sorts on user#${email}. The
        // sort key still has to be rebuilt, or the address survives in it.
        let model = model_with(vec![EntityDefinition {
            name: "User".to_string(),
            pk_template: "user#${id}".to_string(),
            sk_template: Some("user#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![GsiMapping {
                index_name: "GSI1".to_string(),
                pk_template: String::new(),
                sk_template: Some("user#${email}".to_string()),
            }],
            description: None,
        }]);
        let (mut d, _) = KeyDeriver::new(&model, &request(), &[], &no_consistency()).unwrap();
        assert!(
            tracked(&d, 0).contains(&"gs1sk"),
            "the GSI sort key must be tracked: {:?}",
            tracked(&d, 0)
        );

        let mut warnings = Vec::new();
        let mut user = item(&[
            ("pk", "user#u1"),
            ("sk", "user#"),
            ("gs1sk", "user#alice@real.co.uk"),
            ("id", "u1"),
            ("email", "alice@real.co.uk"),
        ]);
        let plan = d.plan(&user, &mut warnings).unwrap();
        user.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);
        assert_eq!(
            user["gs1sk"],
            AttributeValue::S("user#fake@example.org".to_string())
        );
    }

    #[test]
    fn unrelated_pairs_reading_the_same_attribute_name_never_shared_a_key() {
        // Account/Team build account#${id}; Project/Task build project#${id}.
        // They share a key attribute and a source name, so they warn together,
        // but their key *values* never matched, so importing one from each
        // must not read as a broken join.
        let model = model_with(vec![
            entity("Account", "account#${id}", Some("account#"), None),
            entity("Project", "project#${id}", Some("project#"), None),
            entity("Team", "account#${id}", Some("team#"), None),
            entity("Task", "project#${id}", Some("task#"), None),
        ]);
        let rules = [rule(
            "id",
            ValidatedAction::Fake {
                generator: "word".into(),
                seed: None,
            },
        )];
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();
        // One group per key attribute; the four entities all build pk.
        assert_eq!(warnings.len(), 1, "{warnings:?}");

        // One entity from each group, sharing an original id.
        let mut run = |name: &str, pk: &str, sk: &str, becomes: &str| {
            let mut it = item(&[("_type", name), ("pk", pk), ("sk", sk), ("id", "shared")]);
            let mut w = Vec::new();
            let plan = d.plan(&it, &mut w).unwrap();
            it.insert("id".to_string(), AttributeValue::S(becomes.to_string()));
            d.apply(&plan, &no_rewrites(), &mut it, &mut w);
        };
        run("Account", "account#shared", "account#", "anon1");
        run("Project", "project#shared", "project#", "anon2");

        assert!(
            d.join_breaks().is_empty(),
            "different groups never joined: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn a_nested_template_source_is_tracked_for_join_breaks() {
        let model = model_with(vec![
            entity(
                "Customer",
                "CUSTOMER#${contact.email}",
                Some("PROFILE"),
                None,
            ),
            entity(
                "Order",
                "CUSTOMER#${contact.email}",
                Some("ORDER#${id}"),
                None,
            ),
        ]);
        let rules = [rule(
            "contact",
            ValidatedAction::Fake {
                generator: "safe_email".into(),
                seed: None,
            },
        )];
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("('contact')")),
            "the root is what [consistency] honours: {warnings:?}"
        );

        let contact = |email: &str| {
            let mut map = std::collections::HashMap::new();
            map.insert("email".to_string(), AttributeValue::S(email.to_string()));
            AttributeValue::M(map)
        };
        let mut run = |name: &str, sk: &str, becomes: &str| {
            let mut it = item(&[("_type", name), ("pk", "CUSTOMER#a@x.co"), ("sk", sk)]);
            it.insert("contact".to_string(), contact("a@x.co"));
            let mut w = Vec::new();
            let plan = d.plan(&it, &mut w).unwrap();
            it.insert("contact".to_string(), contact(becomes));
            d.apply(&plan, &no_rewrites(), &mut it, &mut w);
        };
        run("Customer", "PROFILE", "fake1@example.com");
        run("Order", "ORDER#1", "fake2@example.org");

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert!(
            breaks[0].contains("Add 'contact' to [consistency] fields"),
            "must name the root, which is what the consistency map keys on: {breaks:?}"
        );
    }

    #[test]
    fn a_numeric_partition_key_is_part_of_the_collision_fingerprint() {
        let model = model_with(vec![entity("Row", "${n}", Some("user#${email}"), None)]);
        let (mut d, _) = KeyDeriver::new(&model, &request(), &[], &no_consistency()).unwrap();
        let mut warnings = Vec::new();

        // Two rows differing only by a numeric pk: distinct, not a collision.
        for n in ["1", "2"] {
            let mut it = Item::new();
            it.insert("_type".to_string(), AttributeValue::S("Row".to_string()));
            it.insert("pk".to_string(), AttributeValue::N(n.to_string()));
            it.insert("n".to_string(), AttributeValue::N(n.to_string()));
            it.insert(
                "sk".to_string(),
                AttributeValue::S("user#a@x.co".to_string()),
            );
            it.insert("email".to_string(), AttributeValue::S("a@x.co".to_string()));
            let plan = d.plan(&it, &mut warnings).unwrap();
            d.apply(&plan, &no_rewrites(), &mut it, &mut warnings);
        }
        assert_eq!(d.take_collisions(), (0, false));
    }

    /// Run one item through the pipeline the way the importer does: plan,
    /// let the rules rewrite `email` to `becomes`, then apply.
    fn run_item(d: &mut KeyDeriver, mut item: Item, becomes: &str) {
        let mut warnings = Vec::new();
        let plan = d.plan(&item, &mut warnings).unwrap();
        item.insert("email".to_string(), AttributeValue::S(becomes.to_string()));
        d.apply(&plan, &no_rewrites(), &mut item, &mut warnings);
    }

    /// Two entities that both sort a GSI on `CODE#${code}`, hashed on tenant.
    fn code_model() -> DataModel {
        let entity = |name: &str, prefix: &str| EntityDefinition {
            name: name.to_string(),
            pk_template: format!("{prefix}#${{id}}"),
            sk_template: Some(format!("{prefix}#")),
            type_attribute: None,
            gsi_mappings: vec![GsiMapping {
                index_name: "GSI1".to_string(),
                pk_template: "TENANT#${tenant}".to_string(),
                sk_template: Some("CODE#${code}".to_string()),
            }],
            description: None,
        };
        model_with(vec![entity("Event", "EVENT"), entity("Alert", "ALERT")])
    }

    fn code_rule() -> [ValidatedRule; 1] {
        [rule(
            "code",
            ValidatedAction::Fake {
                generator: "safe_email".into(),
                seed: None,
            },
        )]
    }

    fn code_item(prefix: &str, id: &str, tenant: &str, code: &str) -> Item {
        item(&[
            ("pk", &format!("{prefix}#{id}")),
            ("sk", &format!("{prefix}#")),
            ("gs1pk", &format!("TENANT#{tenant}")),
            ("gs1sk", &format!("CODE#{code}")),
            ("id", id),
            ("tenant", tenant),
            ("code", code),
        ])
    }

    /// One entity, alone, templating a GSI partition key from `email`.
    fn lone_entity_model() -> DataModel {
        model_with(vec![EntityDefinition {
            name: "Order".to_string(),
            pk_template: "ORDER#${id}".to_string(),
            sk_template: Some("ORDER#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![GsiMapping {
                index_name: "GSI1".to_string(),
                pk_template: "CUSTOMER#${email}".to_string(),
                sk_template: Some("ORDER#${id}".to_string()),
            }],
            description: None,
        }])
    }

    fn lone_item(id: &str, email: &str) -> Item {
        item(&[
            ("pk", &format!("ORDER#{id}")),
            ("sk", "ORDER#"),
            ("gs1pk", &format!("CUSTOMER#{email}")),
            ("gs1sk", &format!("ORDER#{id}")),
            ("id", id),
            ("email", email),
        ])
    }

    #[test]
    fn the_only_entity_that_templates_a_key_is_still_checked() {
        // The commonest single-table shape: one entity builds the customer
        // partition and nothing else does. The check used to require a second
        // entity templating the same attribute before it formed a group at
        // all, so this shape, which is most of the real ones, went unexamined
        // however badly it was split.
        let (mut d, _) = KeyDeriver::new(
            &lone_entity_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();

        run_item_on(
            &mut d,
            lone_item("o1", "a@x.co"),
            "email",
            "fake1@example.org",
        );
        run_item_on(
            &mut d,
            lone_item("o2", "a@x.co"),
            "email",
            "fake2@example.org",
        );

        let breaks = d.join_breaks();
        assert_eq!(
            breaks.len(),
            1,
            "two orders arrived in one customer partition and left in two: {breaks:?}"
        );
        assert!(breaks[0].contains("entity 'Order'"), "{breaks:?}");
        assert!(breaks[0].contains("took one original gs1pk"), "{breaks:?}");
    }

    #[test]
    fn a_lone_entity_whose_rows_never_shared_a_key_is_left_alone() {
        // The same lone entity, but each order belongs to a different
        // customer, so nothing was grouped and nothing is lost. This is what
        // stops the relaxed gate turning every anonymised import into an error.
        let (mut d, _) = KeyDeriver::new(
            &lone_entity_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();

        run_item_on(
            &mut d,
            lone_item("o1", "a@x.co"),
            "email",
            "fake1@example.org",
        );
        run_item_on(
            &mut d,
            lone_item("o2", "b@x.co"),
            "email",
            "fake2@example.org",
        );

        assert!(
            d.join_breaks().is_empty(),
            "different customers never shared a partition: {:?}",
            d.join_breaks()
        );
    }

    /// One entity, alone, sorting a GSI on `CODE#${code}`.
    fn lone_code_model() -> DataModel {
        model_with(vec![EntityDefinition {
            name: "Event".to_string(),
            pk_template: "EVENT#${id}".to_string(),
            sk_template: Some("EVENT#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![GsiMapping {
                index_name: "GSI1".to_string(),
                pk_template: "TENANT#${tenant}".to_string(),
                sk_template: Some("CODE#${code}".to_string()),
            }],
            description: None,
        }])
    }

    #[test]
    fn one_entity_splitting_a_gsi_sort_key_inside_one_partition_is_a_break() {
        // A GSI sort key is not unique, so two rows really can share one
        // inside a partition, and a range query over that index returns them
        // together. Splitting them breaks that, and no second entity has to
        // be present for it to happen: the model here has exactly one, so the
        // assertion cannot be satisfied by a sibling forming the group.
        let (mut d, _) = KeyDeriver::new(
            &lone_code_model(),
            &request(),
            &code_rule(),
            &no_consistency(),
        )
        .unwrap();

        run_item_on(
            &mut d,
            code_item("EVENT", "e1", "t1", "c1"),
            "code",
            "fake1",
        );
        run_item_on(
            &mut d,
            code_item("EVENT", "e2", "t1", "c1"),
            "code",
            "fake2",
        );

        // Reported, not fatal. Rows share a sort value for reasons that are
        // not relationships: two people of one name under one tenant share
        // `CODE#...` and were never related, so failing the import here would
        // block a legitimate export and the only way past is to drop the data
        // model, which ships the real values.
        assert!(
            d.join_breaks().is_empty(),
            "a sort key split is not a broken partition: {:?}",
            d.join_breaks()
        );
        let splits = d.sort_key_splits();
        assert_eq!(splits.len(), 1, "but it is worth saying: {splits:?}");
        assert!(splits[0].contains("took one original gs1sk"), "{splits:?}");
        assert!(splits[0].contains("entity 'Event'"), "{splits:?}");
    }

    #[test]
    fn one_entity_splitting_a_sort_key_across_partitions_is_not_a_break() {
        // The same split, but the rows sit in different GSI partitions, so
        // they were never returned together and there is no join to lose.
        // Reading the sort value alone called this a break and failed an
        // import that had nothing wrong with it.
        let (mut d, _) = KeyDeriver::new(
            &lone_code_model(),
            &request(),
            &code_rule(),
            &no_consistency(),
        )
        .unwrap();

        run_item_on(
            &mut d,
            code_item("EVENT", "e1", "t1", "c1"),
            "code",
            "fake1",
        );
        run_item_on(
            &mut d,
            code_item("EVENT", "e2", "t2", "c1"),
            "code",
            "fake2",
        );

        assert!(
            d.join_breaks().is_empty(),
            "different partitions never shared a group: {:?}",
            d.join_breaks()
        );
        assert!(
            d.sort_key_splits().is_empty(),
            "and nothing was split, so there is nothing to report: {:?}",
            d.sort_key_splits()
        );
    }

    #[test]
    fn two_spellings_of_one_numeric_key_are_one_row() {
        // The database normalises a number on write, so `1` and `1.0` land on
        // one row and the second overwrites the first. Hashing the spelling
        // counted them as two rows and the overwrite went unreported.
        let mut request = request();
        request.key_schema = vec![key_schema("id", KeyType::HASH)];
        request.attribute_definitions = vec![crate::types::AttributeDefinition {
            attribute_name: "id".to_string(),
            attribute_type: crate::types::ScalarAttributeType::N,
        }];
        let (mut d, _) = KeyDeriver::new(&model(), &request, &[], &no_consistency()).unwrap();

        let mut a = Item::new();
        a.insert("id".to_string(), AttributeValue::N("1".to_string()));
        let mut b = Item::new();
        b.insert("id".to_string(), AttributeValue::N("1.0".to_string()));
        d.note_primary_key(&a);
        d.note_primary_key(&b);

        assert_eq!(
            d.take_collisions().0,
            1,
            "one row written twice is one collision"
        );
    }

    #[test]
    fn an_item_several_entities_could_own_is_reported() {
        // Two entities with the same templates and no type attribute on the
        // item: the first wins, which may be wrong, and used to be silent.
        let twin = |name: &str| EntityDefinition {
            name: name.to_string(),
            pk_template: "thing#${id}".to_string(),
            sk_template: Some("thing#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![],
            description: None,
        };
        let model = model_with(vec![twin("Left"), twin("Right")]);
        let (mut d, _) = KeyDeriver::new(&model, &request(), &[], &no_consistency()).unwrap();

        let it = item(&[("pk", "thing#1"), ("sk", "thing#"), ("id", "1")]);
        let mut warnings = Vec::new();
        let plan = d
            .plan(&it, &mut warnings)
            .expect("the first entity is taken");
        assert_eq!(plan.entity, 0);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("entity 'Left' and entity 'Right'"),
            "{warnings:?}"
        );
        assert!(
            warnings[0].contains("taken as entity 'Left'"),
            "{warnings:?}"
        );

        // Once per set of entities, not once per item.
        let mut again = Vec::new();
        d.plan(&it, &mut again);
        assert!(again.is_empty(), "{again:?}");
    }

    #[test]
    fn take_notices_decides_each_concern_in_one_place() {
        use super::super::notice::Concern;
        // Rows whose keys kept their original values are exposures: the run
        // saw a value survive. A split sort key is a caution: nothing
        // survived, a query merely changed shape. These decisions used to be
        // made at the call site in mod.rs; now they are made here, once.
        let (mut d, _) = KeyDeriver::new(&model(), &request(), &[], &no_consistency()).unwrap();
        let mut w = Vec::new();
        let _ = d.plan(
            &item(&[
                ("_type", "User"),
                ("pk", "somethingelse#1"),
                ("sk", "user#a@x.co"),
                ("email", "a@x.co"),
            ]),
            &mut w,
        );
        let notices = d.take_notices();
        let kept = notices
            .iter()
            .find(|n| n.contains("kept the original"))
            .unwrap_or_else(|| panic!("{notices:?}"));
        assert_eq!(kept.concern, Concern::Exposure);
        assert!(
            !kept.contains("table '"),
            "the table is named once, where the notice is filed: {kept:?}"
        );

        let (mut d, _) = KeyDeriver::new(
            &lone_code_model(),
            &request(),
            &code_rule(),
            &no_consistency(),
        )
        .unwrap();
        run_item_on(
            &mut d,
            code_item("EVENT", "e1", "t1", "c1"),
            "code",
            "fake1",
        );
        run_item_on(
            &mut d,
            code_item("EVENT", "e2", "t1", "c1"),
            "code",
            "fake2",
        );
        let notices = d.take_notices();
        let split = notices
            .iter()
            .find(|n| n.contains("took one original gs1sk"))
            .unwrap_or_else(|| panic!("{notices:?}"));
        assert_eq!(split.concern, Concern::Caution);

        assert!(d.take_notices().is_empty(), "taking clears");
    }

    #[test]
    fn unrebuilt_keys_are_counted_per_entity_and_key() {
        // One warning, a real number behind it. Reporting "at least one item"
        // read the same whether one stray row or the whole table kept the
        // value it arrived with, and those keys hold what the run exists to
        // remove.
        let (mut d, _) = KeyDeriver::new(&model(), &request(), &[], &no_consistency()).unwrap();

        // Three users whose pk the template cannot reproduce.
        for n in 0..3 {
            let mut warnings = Vec::new();
            let it = item(&[
                ("_type", "User"),
                ("pk", &format!("somethingelse#{n}")),
                ("sk", "user#"),
                ("email", "a@x.co"),
            ]);
            let _ = d.plan(&it, &mut warnings);
        }

        // A fourth item whose template does match, made unrenderable after the
        // rules by turning `email` into a map. User's sk reads `email`, so the
        // same (User, sk) lands in both maps and the merge has something to
        // merge; its pk reads `accountId` and is untouched by this.
        let mut u = user();
        let mut w = Vec::new();
        let plan = d
            .plan(&u, &mut w)
            .expect("the template reproduces this one");
        u.insert(
            "email".to_string(),
            AttributeValue::M(std::collections::HashMap::new()),
        );
        d.apply(&plan, &no_rewrites(), &mut u, &mut w);

        let counts = d.take_unrebuilt();
        assert!(!counts.is_empty(), "the keys were not rebuilt");
        let sk_row = counts
            .iter()
            .find(|(e, k, _)| e == "User" && k == "sk")
            .expect("a row for User/sk");
        assert_eq!(
            sk_row.2, 4,
            "three mismatches and one unrenderable are one fact: {counts:?}"
        );
        // One row per (entity, key). The two causes, a template that does not
        // reproduce and a key that cannot render after the rules, describe the
        // same outcome and used to be listed twice with two partial counts.
        let mut pairs: Vec<(&String, &String)> = counts.iter().map(|(e, k, _)| (e, k)).collect();
        let before = pairs.len();
        pairs.sort();
        pairs.dedup();
        assert_eq!(
            pairs.len(),
            before,
            "one row per entity and key: {counts:?}"
        );

        assert!(
            d.take_unrebuilt().is_empty(),
            "taking clears, so a second table does not re-report the first"
        );
    }

    #[test]
    fn two_people_who_merely_share_a_sort_value_do_not_fail_the_import() {
        // The over-fire direction. Rows land on one sort value for reasons
        // that are not relationships, so aborting here would block a
        // legitimate export and the only way past is to drop --data-model,
        // which turns key rebuilding off and ships the real values. The guard
        // being useful depends on it not crying wolf.
        let (mut d, _) = KeyDeriver::new(
            &lone_code_model(),
            &request(),
            &code_rule(),
            &no_consistency(),
        )
        .unwrap();

        run_item_on(
            &mut d,
            code_item("EVENT", "e1", "t1", "same"),
            "code",
            "fake1",
        );
        run_item_on(
            &mut d,
            code_item("EVENT", "e2", "t1", "same"),
            "code",
            "fake2",
        );

        assert!(
            d.join_breaks().is_empty(),
            "a shared sort value is not a shared partition: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn two_indexes_holding_the_same_partition_string_are_separate_groups() {
        // Two GSIs whose partition values spell the same thing. Bucketing on
        // the value alone put them in one group, so a split under one index
        // read as a disagreement across both and failed an import that had
        // nothing wrong with it.
        let model = model_with(vec![EntityDefinition {
            name: "Event".to_string(),
            pk_template: "EVENT#${id}".to_string(),
            sk_template: Some("EVENT#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![
                GsiMapping {
                    index_name: "GSI1".to_string(),
                    pk_template: "T#${tenant}".to_string(),
                    sk_template: Some("CODE#${code}".to_string()),
                },
                GsiMapping {
                    index_name: "GSI2".to_string(),
                    pk_template: "T#${region}".to_string(),
                    sk_template: Some("CODE#${code}".to_string()),
                },
            ],
            description: None,
        }]);
        let (d, _) = KeyDeriver::new(
            &model,
            &two_index_request(),
            &code_rule(),
            &no_consistency(),
        )
        .unwrap();

        // Both indexes' partitions are tracked for the shared sort attribute,
        // rather than only the first one found.
        let groupings = d
            .at_risk
            .iter()
            .find(|r| r.key_attribute == "gs1sk")
            .map(|r| r.grouped_by.clone())
            .unwrap_or_default();
        assert_eq!(
            groupings.len(),
            2,
            "a sort key shared by two indexes has two groupings: {groupings:?}"
        );

        // And the bucket itself: two rows whose GSI1 and GSI2 partitions
        // spell the same string the other way round share no group in
        // either index, so splitting their code costs nothing. Bucketing on
        // the value alone put item 1's GSI2 row and item 2's GSI1 row in one
        // bucket and reported a split that never happened.
        let mut d = d;
        let row = |id: &str, tenant: &str, region: &str| {
            item(&[
                ("pk", &format!("EVENT#{id}")),
                ("sk", "EVENT#"),
                ("gs1pk", &format!("T#{tenant}")),
                ("gs1sk", "CODE#c1"),
                ("gs2pk", &format!("T#{region}")),
                ("id", id),
                ("tenant", tenant),
                ("region", region),
                ("code", "c1"),
            ])
        };
        run_item_on(&mut d, row("e1", "a", "b"), "code", "fake1");
        run_item_on(&mut d, row("e2", "b", "a"), "code", "fake2");
        assert!(
            d.join_breaks().is_empty(),
            "different groups in every index: {:?}",
            d.join_breaks()
        );
        assert!(
            d.sort_key_splits().is_empty(),
            "and no split to report either: {:?}",
            d.sort_key_splits()
        );
    }

    #[test]
    fn a_local_secondary_index_is_a_warning_not_an_exposure() {
        // The model says nothing about an LSI, so nothing says its sort key
        // embeds an attribute a rule touches. Reporting it is right; making
        // every LSI table exit 3 was not.
        let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
            "TableName": "App",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "lsisk", "AttributeType": "S"}
            ],
            "LocalSecondaryIndexes": [{
                "IndexName": "LSI1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsisk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .unwrap();
        // No GSI mapping, so the only thing the model can be told about the
        // table is the LSI. (An index the model describes but the table lacks
        // is a different case, and is meant to be an exposure.)
        let model = model_with(vec![EntityDefinition {
            name: "User".to_string(),
            pk_template: "user#${id}".to_string(),
            sk_template: Some("user#${email}".to_string()),
            type_attribute: None,
            gsi_mappings: vec![],
            description: None,
        }]);
        let (d, warnings) =
            KeyDeriver::new(&model, &request, &email_rule(), &no_consistency()).unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("local secondary index")),
            "it is said: {warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .all(|w| w.concern != super::super::notice::Concern::Exposure),
            "but it is not an observed exposure: {warnings:?}"
        );
        let _ = d;
    }

    #[test]
    fn a_nested_rule_target_still_finds_its_value_in_a_key() {
        // The rule is on profile.email. The snapshot used to read only the
        // top-level `profile`, a map, and record nothing, so the address the
        // rule replaced went unseen in the untemplated gs1pk.
        let model = model_with(vec![EntityDefinition {
            name: "Order".to_string(),
            pk_template: "ORDER#${id}".to_string(),
            sk_template: Some("ORDER#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![],
            description: None,
        }]);
        let rules = [rule(
            "profile.email",
            ValidatedAction::Fake {
                generator: "safe_email".into(),
                seed: None,
            },
        )];
        let (mut d, _) = KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();

        let mut profile = std::collections::HashMap::new();
        profile.insert(
            "email".to_string(),
            AttributeValue::S("alice@real.co.uk".to_string()),
        );
        let mut it = item(&[
            ("pk", "ORDER#o1"),
            ("sk", "ORDER#"),
            ("gs1pk", "CUSTOMER#alice@real.co.uk"),
            ("id", "o1"),
        ]);
        it.insert("profile".to_string(), AttributeValue::M(profile));

        let mut warnings = Vec::new();
        let plan = d.plan(&it, &mut warnings).unwrap();
        let mut replaced = std::collections::HashMap::new();
        replaced.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        it.insert("profile".to_string(), AttributeValue::M(replaced));
        let mut rewritten = HashSet::new();
        rewritten.insert("profile".to_string());
        d.apply(&plan, &rewritten, &mut it, &mut warnings);

        assert_eq!(
            d.take_key_survivals(),
            vec![("Order".to_string(), "gs1pk".to_string(), 1)]
        );
    }

    #[test]
    fn a_value_has_to_sit_in_the_key_as_a_component_to_count() {
        // id=42 was replaced. ORDER#1042 merely contains the digits; it does
        // not hold the id. STATUS#42 does.
        assert!(!holds_as_component("ORDER#1042", "42"));
        assert!(holds_as_component("STATUS#42", "42"));
        assert!(holds_as_component("CUSTOMER#alice@x", "alice@x"));
        assert!(holds_as_component("alice@x", "alice@x"));
        assert!(!holds_as_component("CUSTOMER#alice@x", ""));
    }

    #[test]
    fn a_rule_for_another_table_does_not_shape_this_tables_keys() {
        // The rule names pk, but only for Orders. On this table pk is not
        // rule-owned, so a template that fails to reproduce it is counted
        // rather than excused.
        let mut scoped = rule("pk", ValidatedAction::Redact);
        scoped.tables = Some(vec!["Orders".to_string()]);
        let (mut d, _) =
            KeyDeriver::new(&model(), &request(), &[scoped], &no_consistency()).unwrap();

        let it = item(&[
            ("_type", "User"),
            ("pk", "somethingelse#1"),
            ("sk", "user#a@x.co"),
            ("email", "a@x.co"),
        ]);
        let mut warnings = Vec::new();
        let _ = d.plan(&it, &mut warnings);
        assert!(
            d.take_unrebuilt()
                .iter()
                .any(|(e, k, _)| e == "User" && k == "pk"),
            "pk did not reproduce and no applicable rule owns it"
        );
    }

    #[test]
    fn a_direct_key_rule_on_a_key_no_template_reproduces_is_still_join_checked() {
        // Both entities arrived sharing pk = legacy#same, which neither
        // template reproduces. A rule then rewrites pk with an unseeded fake
        // and the two leave with different keys. That is a broken join, and
        // a key excused from the mismatch count was also excused from this.
        let rules = [rule(
            "pk",
            ValidatedAction::Fake {
                generator: "word".into(),
                seed: None,
            },
        )];
        let (mut d, _) =
            KeyDeriver::new(&shared_key_model(), &request(), &rules, &no_consistency()).unwrap();

        let mut rewritten = HashSet::new();
        rewritten.insert("pk".to_string());
        for (ty, sk, becomes) in [
            ("Customer", "PROFILE", "fake1"),
            ("Order", "ORDER#1", "fake2"),
        ] {
            let mut it = item(&[
                ("_type", ty),
                ("pk", "legacy#same"),
                ("sk", sk),
                ("email", "a@x.co"),
            ]);
            let mut w = Vec::new();
            let plan = d.plan(&it, &mut w).unwrap();
            it.insert("pk".to_string(), AttributeValue::S(becomes.to_string()));
            d.apply(&plan, &rewritten, &mut it, &mut w);
        }

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "the shared key split: {breaks:?}");
    }

    #[test]
    fn a_key_the_model_never_templates_that_still_holds_a_replaced_value_is_counted() {
        // The entity's model says nothing about gs1pk, so it is not rebuilt.
        // The item carries it, built from the email a rule then replaced. The
        // real address is in a key on a run that would otherwise pass.
        let model = model_with(vec![EntityDefinition {
            name: "Order".to_string(),
            pk_template: "ORDER#${id}".to_string(),
            sk_template: Some("ORDER#".to_string()),
            type_attribute: None,
            gsi_mappings: vec![],
            description: None,
        }]);
        let (mut d, _) =
            KeyDeriver::new(&model, &request(), &email_rule(), &no_consistency()).unwrap();

        let mut it = item(&[
            ("pk", "ORDER#o1"),
            ("sk", "ORDER#"),
            ("gs1pk", "CUSTOMER#alice@real.co.uk"),
            ("id", "o1"),
            ("email", "alice@real.co.uk"),
        ]);
        let mut warnings = Vec::new();
        let plan = d.plan(&it, &mut warnings).unwrap();
        it.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        let mut rewritten = HashSet::new();
        rewritten.insert("email".to_string());
        d.apply(&plan, &rewritten, &mut it, &mut warnings);

        assert!(
            warnings
                .iter()
                .any(|w| w.contains("gs1pk still holds a value a rule replaced")),
            "{warnings:?}"
        );
        assert_eq!(
            d.take_key_survivals(),
            vec![("Order".to_string(), "gs1pk".to_string(), 1)]
        );

        // The other direction: a key holding nothing a rule touched is not
        // counted, or every untemplated index would raise this.
        let mut clean = item(&[
            ("pk", "ORDER#o2"),
            ("sk", "ORDER#"),
            ("gs1pk", "STATUS#open"),
            ("id", "o2"),
            ("email", "bob@real.co.uk"),
        ]);
        let plan = d.plan(&clean, &mut warnings).unwrap();
        clean.insert(
            "email".to_string(),
            AttributeValue::S("fake2@example.org".to_string()),
        );
        d.apply(&plan, &rewritten, &mut clean, &mut warnings);
        assert!(d.take_key_survivals().is_empty());
    }

    #[test]
    fn two_entities_sharing_a_gsi_partition_and_sort_value_still_break() {
        // The cross-entity case the check already covered, now that the
        // bucket carries the partition too.
        let (mut d, _) =
            KeyDeriver::new(&code_model(), &request(), &code_rule(), &no_consistency()).unwrap();

        run_item_on(
            &mut d,
            code_item("EVENT", "e1", "t1", "c1"),
            "code",
            "fake1",
        );
        run_item_on(
            &mut d,
            code_item("ALERT", "a1", "t1", "c1"),
            "code",
            "fake2",
        );

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert!(
            breaks[0].contains("entity 'Alert' and entity 'Event'")
                || breaks[0].contains("entity 'Event' and entity 'Alert'"),
            "{breaks:?}"
        );
    }

    #[test]
    fn padding_matches_javascript_pad_start() {
        // OneTable renders these in JavaScript, so `padStart` is the oracle.
        // Every expectation below was taken from node rather than reasoned
        // out. The old implementation prepended the fill whole and stopped
        // once it was long enough, so a multi-character fill overshot and the
        // rendered key stopped matching the model; a key the template cannot
        // reproduce is left alone, keeping the value it arrived with.
        let cases = [
            ("7", 4, "0", "0007"),
            ("7", 4, "00", "0007"),
            ("xx", 5, "ab", "abaxx"),
            ("42", 6, "-*", "-*-*42"),
            ("abc", 2, "0", "abc"),
            ("", 3, "xyz", "xyz"),
            ("日本", 4, "0", "00日本"),
            // Counted in UTF-16 units, as JavaScript counts them: this
            // emoji is two there and one `char` here.
            ("\u{1F600}", 4, "0", "00\u{1F600}"),
            ("a\u{1F600}", 4, "0", "0a\u{1F600}"),
        ];
        for (value, length, fill, want) in cases {
            let pad = Padding {
                length,
                fill: fill.to_string(),
            };
            assert_eq!(
                pad.apply(value),
                want,
                "padStart({value:?}, {length}, {fill:?})"
            );
        }
    }

    fn run_item_on(d: &mut KeyDeriver, mut item: Item, attribute: &str, becomes: &str) {
        let mut warnings = Vec::new();
        let plan = d.plan(&item, &mut warnings).unwrap();
        item.insert(
            attribute.to_string(),
            AttributeValue::S(becomes.to_string()),
        );
        d.apply(&plan, &no_rewrites(), &mut item, &mut warnings);
    }

    fn shared_key_deriver() -> KeyDeriver {
        KeyDeriver::new(
            &shared_key_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap()
        .0
    }

    fn customer_item(email: &str) -> Item {
        item(&[
            ("_type", "Customer"),
            ("pk", &format!("CUSTOMER#{email}")),
            ("sk", "PROFILE"),
            ("email", email),
        ])
    }

    fn order_item(email: &str) -> Item {
        item(&[
            ("_type", "Order"),
            ("pk", &format!("CUSTOMER#{email}")),
            ("sk", "ORDER#1"),
            ("email", email),
            ("orderId", "1"),
        ])
    }

    #[test]
    fn two_entities_anonymising_a_shared_value_differently_is_a_broken_join() {
        let mut d = shared_key_deriver();

        run_item(&mut d, customer_item("a@x.co"), "fake1@example.com");
        assert!(d.join_breaks().is_empty(), "one entity so far");

        // Same original address, a different fake: the join is gone.
        run_item(&mut d, order_item("a@x.co"), "fake2@example.org");

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert!(breaks[0].contains("entity 'Customer' and entity 'Order'"));
        assert!(breaks[0].contains("Add 'email' to [consistency] fields"));
    }

    #[test]
    fn a_break_is_found_whatever_order_the_export_is_in() {
        // Two Orders share a customer. Only the second is rewritten, and the
        // Customer arrives last. Keeping just the first outcome per original
        // would let the Customer match the untouched one and pass.
        let mut d = shared_key_deriver();

        run_item(&mut d, order_item("a@x.co"), "a@x.co"); // unchanged
        run_item(&mut d, order_item("a@x.co"), "fake@example.org"); // rewritten
        run_item(&mut d, customer_item("a@x.co"), "a@x.co"); // unchanged

        let breaks = d.join_breaks();
        assert_eq!(
            breaks.len(),
            1,
            "the rewritten order lost its customer: {breaks:?}"
        );
    }

    #[test]
    fn two_composite_keys_sharing_only_a_component_never_joined() {
        // Both entities build pk from TENANT#${tenantId}#${email}. A customer
        // in one tenant and an order in another share an address but never
        // shared a partition, so different fakes cost them nothing.
        let model = model_with(vec![
            entity(
                "Customer",
                "TENANT#${tenantId}#${email}",
                Some("PROFILE"),
                None,
            ),
            entity(
                "Order",
                "TENANT#${tenantId}#${email}",
                Some("ORDER#${id}"),
                None,
            ),
        ]);
        let (mut d, _) =
            KeyDeriver::new(&model, &request(), &email_rule(), &no_consistency()).unwrap();

        let mut run = |name: &str, tenant: &str, sk: &str, becomes: &str| {
            let mut it = item(&[
                ("_type", name),
                ("pk", &format!("TENANT#{tenant}#a@x.co")),
                ("sk", sk),
                ("tenantId", tenant),
                ("email", "a@x.co"),
                ("id", "1"),
            ]);
            let mut w = Vec::new();
            let plan = d.plan(&it, &mut w).unwrap();
            it.insert("email".to_string(), AttributeValue::S(becomes.to_string()));
            d.apply(&plan, &no_rewrites(), &mut it, &mut w);
        };
        run("Customer", "a", "PROFILE", "fake1@example.com");
        run("Order", "b", "ORDER#1", "fake2@example.org");

        assert!(
            d.join_breaks().is_empty(),
            "different tenants never shared a key: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn one_entity_repeating_a_key_keeps_a_bounded_summary() {
        // Many orders share one customer key and each gets its own fake, so
        // keeping every outcome would grow without limit and be rescanned
        // per item. One summary row per entity is enough.
        let mut d = shared_key_deriver();
        for n in 0..50 {
            run_item(
                &mut d,
                order_item("a@x.co"),
                &format!("fake{n}@example.org"),
            );
        }

        let outcomes: usize = d
            .at_risk
            .iter()
            .map(|r| r.seen_keys.values().map(Vec::len).sum::<usize>())
            .sum();
        assert_eq!(outcomes, 1, "one row per entity per key, not per item");

        // Fifty orders arrived under one customer partition and left under
        // fifty, so the rows that shared a partition no longer do. No second
        // entity is needed for that to be a broken join, and the summary row
        // above is what carries it: the outcome records the split, so the
        // check reads it rather than only comparing one entity against another.
        let breaks = d.join_breaks();
        assert_eq!(
            breaks.len(),
            1,
            "an entity that splits its own partition has broken a join: {breaks:?}"
        );
        assert!(
            breaks[0].contains("took one original"),
            "the message should name the split rather than a disagreement \
             between two entities: {breaks:?}"
        );

        // The customer still finds the disagreement.
        run_item(&mut d, customer_item("a@x.co"), "different@example.com");
        assert_eq!(d.join_breaks().len(), 1);
    }

    #[test]
    fn keys_seen_before_the_cap_are_still_checked_after_it() {
        let mut d = shared_key_deriver();
        // Record the customer, then fill the map to its cap.
        run_item(&mut d, customer_item("a@x.co"), "fake1@example.com");
        for risk in &mut d.at_risk {
            while risk.seen_keys.len() < MAX_TRACKED_KEYS {
                let filler = risk.seen_keys.len() as u64 + 1_000_000;
                risk.seen_keys.insert(filler, Vec::new());
            }
        }

        // A brand new key cannot be taken on, and is counted.
        run_item(&mut d, order_item("b@y.co"), "fake2@example.org");
        assert!(d.unchecked_join_keys() > 0, "the new key went unchecked");

        // The key recorded before the cap is still compared.
        run_item(&mut d, order_item("a@x.co"), "different@example.org");
        assert_eq!(
            d.join_breaks().len(),
            1,
            "a key seen before the cap must still be checked: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn a_join_across_two_different_templates_is_still_seen() {
        // The canonical single-table join: the customer keys on its own
        // address, the order keys on the customer's. Different template text,
        // same partition. Matching on the text would miss it entirely.
        let model = model_with(vec![
            entity("Customer", "CUSTOMER#${email}", Some("PROFILE"), None),
            entity(
                "Order",
                "CUSTOMER#${customerEmail}",
                Some("ORDER#${id}"),
                None,
            ),
        ]);
        let rules = [
            rule(
                "email",
                ValidatedAction::Fake {
                    generator: "safe_email".into(),
                    seed: None,
                },
            ),
            rule(
                "customerEmail",
                ValidatedAction::Fake {
                    generator: "safe_email".into(),
                    seed: None,
                },
            ),
        ];
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("entity 'Customer' and entity 'Order'"));

        let mut run = |name: &str, source: &str, sk: &str, becomes: &str| {
            let mut it = item(&[
                ("_type", name),
                ("pk", "CUSTOMER#a@x.co"),
                ("sk", sk),
                (source, "a@x.co"),
                ("id", "1"),
            ]);
            let mut w = Vec::new();
            let plan = d.plan(&it, &mut w).unwrap();
            it.insert(source.to_string(), AttributeValue::S(becomes.to_string()));
            d.apply(&plan, &no_rewrites(), &mut it, &mut w);
        };
        run("Customer", "email", "PROFILE", "fake1@example.com");
        run("Order", "customerEmail", "ORDER#1", "fake2@example.org");

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert!(breaks[0].contains("same attribute name"), "{breaks:?}");
    }

    #[test]
    fn a_sibling_whose_source_no_rule_rewrites_still_breaks_the_join() {
        // Only the customer's address has a rule. The order keys on the same
        // address under its own name, which nothing rewrites, so its key keeps
        // the real value while the customer's moves. That is the join breaking
        // in the most ordinary way, and it is only visible by recording what
        // the untouched entity's key stayed as.
        let model = model_with(vec![
            entity("Customer", "CUSTOMER#${email}", Some("PROFILE"), None),
            entity(
                "Order",
                "CUSTOMER#${customerEmail}",
                Some("ORDER#${id}"),
                None,
            ),
        ]);
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &email_rule(), &no_consistency()).unwrap();
        assert!(
            warnings.is_empty(),
            "one entity at risk has nothing to list up front: {warnings:?}"
        );

        let mut run = |name: &str, source: &str, sk: &str, becomes: Option<&str>| {
            let mut it = item(&[
                ("_type", name),
                ("pk", "CUSTOMER#a@x.co"),
                ("sk", sk),
                (source, "a@x.co"),
                ("id", "1"),
            ]);
            let mut w = Vec::new();
            let plan = d.plan(&it, &mut w).unwrap();
            if let Some(becomes) = becomes {
                it.insert(source.to_string(), AttributeValue::S(becomes.to_string()));
            }
            d.apply(&plan, &no_rewrites(), &mut it, &mut w);
        };
        run("Customer", "email", "PROFILE", Some("fake1@example.com"));
        run("Order", "customerEmail", "ORDER#1", None);

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert!(
            breaks[0].contains("entity 'Customer' and entity 'Order'"),
            "{breaks:?}"
        );
    }

    #[test]
    fn a_key_left_unrebuilt_is_not_reported_as_a_broken_join() {
        // The documented shape: an Order holds its customer's key but carries
        // no address of its own, so its pk cannot be rebuilt. That is a
        // mismatch warning, not a join failure, and [consistency] could not
        // fix it because there is nothing on the Order to anonymise.
        let mut d = shared_key_deriver();

        run_item(&mut d, customer_item("a@x.co"), "fake1@example.com");

        let mut order = item(&[
            ("_type", "Order"),
            ("pk", "CUSTOMER#a@x.co"),
            ("sk", "ORDER#1"),
            ("orderId", "1"),
        ]);
        assert!(!order.contains_key("email"), "nothing to rebuild pk from");
        let mut warnings = Vec::new();
        let plan = d.plan(&order, &mut warnings).unwrap();
        d.apply(&plan, &no_rewrites(), &mut order, &mut warnings);

        assert!(
            warnings.iter().any(|w| w.contains("does not reproduce pk")),
            "the accurate diagnostic still fires: {warnings:?}"
        );
        assert!(
            d.join_breaks().is_empty(),
            "an unrebuilt key is not a divergence: {:?}",
            d.join_breaks()
        );
        assert_eq!(
            order["pk"],
            AttributeValue::S("CUSTOMER#a@x.co".to_string())
        );
    }

    #[test]
    fn an_index_the_table_does_not_have_is_reported() {
        let model = model_with(vec![EntityDefinition {
            name: "User".to_string(),
            pk_template: "user#${id}".to_string(),
            sk_template: Some("user#".to_string()),
            type_attribute: None,
            // OneTable's default name for the index, which the table calls GSI1.
            gsi_mappings: vec![GsiMapping {
                index_name: "gs1".to_string(),
                pk_template: String::new(),
                sk_template: Some("user#${email}".to_string()),
            }],
            description: None,
        }]);
        let (_, warnings) = KeyDeriver::new(&model, &request(), &[], &no_consistency()).unwrap();
        // The table's name is added once, where the notice is filed; here the
        // message stands alone. And the model itself claims this index's keys
        // are built from attributes, so their surviving is an exposure.
        let unmatched = warnings
            .iter()
            .find(|w| w.contains("index 'gs1' that the table does not"))
            .unwrap_or_else(|| panic!("{warnings:?}"));
        assert_eq!(unmatched.concern, super::super::notice::Concern::Exposure);
    }

    #[test]
    fn a_local_secondary_index_is_reported_as_not_rebuilt() {
        let mut request = request();
        request.local_secondary_indexes = Some(
            serde_json::from_value(serde_json::json!([{
                "IndexName": "LSI1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "ls1sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]))
            .unwrap(),
        );
        let (_, warnings) = KeyDeriver::new(&model(), &request, &[], &no_consistency()).unwrap();
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("local secondary index") && w.contains("not rebuilt")),
            "{warnings:?}"
        );
    }

    #[test]
    fn null_on_a_key_source_is_reported_as_retaining_not_collapsing() {
        let rules = [rule("email", ValidatedAction::Null)];
        let (_, warnings) =
            KeyDeriver::new(&model(), &request(), &rules, &no_consistency()).unwrap();
        assert!(!warnings.is_empty());
        for w in &warnings {
            assert!(
                w.contains("keeps the key value it arrived with"),
                "null retains, it does not collapse: {w}"
            );
            assert!(!w.contains("overwrites the last"), "{w}");
        }
    }

    #[test]
    fn a_rebuilt_key_landing_on_an_untouched_row_is_a_collision() {
        // An item that matches no entity keeps its key and is invisible to a
        // collision set that only records rebuilds, so the row it loses goes
        // uncounted.
        let mut d = deriver();
        let mut warnings = Vec::new();

        let stranger = item(&[("pk", "account#acc1"), ("sk", "user#[REDACTED]")]);
        assert!(
            d.plan(&stranger, &mut warnings).is_none(),
            "matches nothing"
        );
        d.note_primary_key(&stranger);

        let mut user = user();
        let plan = d.plan(&user, &mut warnings).unwrap();
        user.insert(
            "email".to_string(),
            AttributeValue::S("[REDACTED]".to_string()),
        );
        d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);
        assert_eq!(
            user["sk"],
            AttributeValue::S("user#[REDACTED]".to_string()),
            "the rebuilt key lands on the stranger's"
        );
        assert_eq!(d.take_collisions().0, 1);
    }

    #[test]
    fn a_shared_value_anonymised_the_same_way_still_joins() {
        // What a deterministic action such as hash does, or the consistency
        // map, or a rule that matched neither item: same in, same out.
        let mut d = shared_key_deriver();
        run_item(&mut d, customer_item("a@x.co"), "same@example.com");
        run_item(&mut d, order_item("a@x.co"), "same@example.com");
        assert!(
            d.join_breaks().is_empty(),
            "both agreed, so the join survives: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn both_entities_with_no_value_in_common_is_not_a_broken_join() {
        let mut d = shared_key_deriver();
        run_item(&mut d, customer_item("a@x.co"), "fake1@example.com");
        // A different address, so these two never joined.
        run_item(&mut d, order_item("b@y.co"), "fake2@example.org");
        assert!(
            d.join_breaks().is_empty(),
            "both entities present but no shared value: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn rebuilt_primary_keys_that_repeat_are_counted_as_collisions() {
        let mut d = deriver();
        let mut warnings = Vec::new();
        for n in 0..3 {
            let mut user = item(&[
                ("_type", "User"),
                ("pk", "account#acc1"),
                ("sk", &format!("user#u{n}@example.com")),
                ("accountId", "acc1"),
                ("email", &format!("u{n}@example.com")),
            ]);
            let plan = d.plan(&user, &mut warnings).unwrap();
            user.insert(
                "email".to_string(),
                AttributeValue::S("[REDACTED]".to_string()),
            );
            d.apply(&plan, &no_rewrites(), &mut user, &mut warnings);
            assert_eq!(user["sk"], AttributeValue::S("user#[REDACTED]".to_string()));
        }
        assert_eq!(d.take_collisions(), (2, false));
        assert_eq!(d.take_collisions(), (0, false));
    }
}
