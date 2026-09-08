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

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use crate::actions::create_table::CreateTableRequest;
use crate::expressions::{PathElement, resolve_path};
use crate::schema::{DataModel, EntityDefinition};
use crate::types::{AttributeValue, Item};
use crate::validation::{partition_key_name, sort_key_name};

use super::config::{ValidatedAction, ValidatedRule, parse_path};

/// Rebuilt primary keys are remembered (as hashes) to spot two items
/// collapsing onto one row. Past this many the check stops, and says so.
pub(super) const MAX_TRACKED_KEYS: usize = 1_000_000;

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
    fn apply(&self, value: &str) -> String {
        let mut out = value.to_string();
        if self.fill.is_empty() {
            return out;
        }
        while out.chars().count() < self.length {
            out.insert_str(0, &self.fill);
        }
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
                Some(Padding {
                    length,
                    fill: parts.next().unwrap_or("0").to_string(),
                })
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

/// An attribute more than one entity builds a key from, which is not in
/// `[consistency] fields`. Anonymising it independently per item means the
/// entities' keys disagree and the join between them is lost.
#[derive(Debug)]

struct AtRisk {
    /// The key attribute the shared template builds.
    key_attribute: String,
    /// The template itself, for the message.
    template: String,
    /// Root field names to put in `[consistency] fields`. Roots, because
    /// that is what the consistency map is keyed on: advising `contact.email`
    /// when only `contact` is honoured would send someone in a circle.
    consistency_roots: Vec<String>,
    /// Entity indices that build this key from this template, in model order.
    entities: Vec<usize>,
    /// Hash of the whole key value the item arrived with -> every (entity,
    /// resulting key value) seen for it. Comparing the whole key, rather than
    /// one attribute it reads, tells a real join from two composite keys that
    /// merely share a component: `TENANT#a#x` and `TENANT#b#x` never agreed,
    /// so they have nothing to lose. A deterministic action such as hash
    /// takes both to the same place and stays quiet.
    seen_keys: std::collections::HashMap<u64, Vec<(usize, u64)>>,
    /// Entities found to have taken a shared key to different values.
    diverged: HashSet<usize>,
    /// Set once `seen_keys` hit its cap and stopped tracking.
    capped: bool,
}

/// Which of an item's keys can be rebuilt after the rules run.
#[derive(Debug)]
pub struct Rederivation {
    entity: usize,
    keys: Vec<usize>,
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
    warned_mismatch: HashSet<(usize, usize)>,
    /// (entity index, key index) pairs that could not be rendered after the
    /// rules ran, reported once.
    warned_unrenderable: HashSet<(usize, usize)>,
    /// Items that matched no entity, reported once per table.
    unmatched: usize,
    /// (entity index, key index) pairs where a rule took the key instead of
    /// its template, reported once.
    warned_rule_wins: HashSet<(usize, usize)>,
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
    ) -> Result<(Self, Vec<String>), String> {
        let hash = partition_key_name(&request.key_schema);
        let range = sort_key_name(&request.key_schema);
        let gsis = request.global_secondary_indexes.as_deref().unwrap_or(&[]);

        let rule_targets: HashSet<&str> = rules.iter().filter_map(rule_target).collect();
        let mut warnings = Vec::new();

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

            let keys: Vec<TemplatedKey> = match_keys
                .iter()
                .filter(|key| {
                    key.segments
                        .iter()
                        .any(|s| matches!(s, Segment::Var { .. }))
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
                    warnings.push(format!(
                        "a rule targets key attribute '{}' directly, so on any item that rule \
                         matches it is not rebuilt from entity '{}' template '{}' and the \
                         rule's value is kept as written",
                        key.attribute, entity.name, key.template
                    ));
                }
            }

            for key in &keys {
                for rule in rules {
                    let Some(target) = rule_target(rule) else {
                        continue;
                    };
                    let collapses = match rule.action {
                        ValidatedAction::Redact | ValidatedAction::Null => "every",
                        ValidatedAction::Mask { .. } => "most",
                        ValidatedAction::Fake { .. } | ValidatedAction::Hash { .. } => continue,
                    };
                    if key.sources().any(|source| source == target) {
                        warnings.push(format!(
                            "a rule replaces '{target}' with a constant, and entity '{}' builds \
                             {} from template '{}': {collapses} item of that entity would render \
                             the same key and collapse onto one row. Use fake or hash for an \
                             attribute a key is built from",
                            entity.name, key.attribute, key.template
                        ));
                    }
                }
            }

            entities.push(EntityKeys {
                name: entity.name.clone(),
                type_attribute: type_attribute(model, entity),
                keys,
                match_keys,
            });
        }

        // Two entities that build the *same* key attribute from the *same*
        // template are asserting their keys agree, which is how a
        // single-table design keeps a customer and its orders in one
        // partition. If a rule rewrites an attribute that template reads and
        // it is not consistency-tracked, each entity anonymises it
        // independently and the two stop agreeing.
        //
        // Matching on the (attribute, template) pair rather than on the
        // attribute name alone is what keeps this off entities that merely
        // reuse a name: `account#${id}` and `project#${id}` are different
        // entities' own ids, and never had a join to lose.
        // Grouped by the key and its template as well as the source, so two
        // unrelated pairs that happen to read an attribute of the same name
        // stay separate: A and B sharing `account#${id}` have nothing to do
        // with C and D sharing `project#${id}`.
        // Grouped by the key and its template, so two unrelated pairs that
        // happen to read an attribute of the same name stay separate: A and B
        // sharing `account#${id}` have nothing to do with C and D sharing
        // `project#${id}`.
        /// (key attribute, template) -> (entities building it, roots needing consistency)
        type SharedGroups = Vec<((String, String), (Vec<usize>, Vec<String>))>;
        let mut shared: SharedGroups = Vec::new();
        for (idx, shape) in entity_key_shapes.iter().enumerate() {
            for (attribute, template, sources) in shape {
                let shared_with_another =
                    entity_key_shapes
                        .iter()
                        .enumerate()
                        .any(|(other, other_shape)| {
                            other != idx
                                && other_shape
                                    .iter()
                                    .any(|(a, t, _)| a == attribute && t == template)
                        });
                if !shared_with_another {
                    continue;
                }
                // An attribute no rule rewrites keeps its value, so the two
                // keys still agree however they were built.
                let roots: Vec<String> = sources
                    .iter()
                    .filter_map(|source| match source.first() {
                        Some(PathElement::Attribute(root)) => Some(root.clone()),
                        _ => None,
                    })
                    .filter(|root| {
                        !consistency_fields.contains(root) && rule_targets.contains(root.as_str())
                    })
                    .collect();
                if roots.is_empty() {
                    continue;
                }
                let group = (attribute.clone(), template.clone());
                match shared.iter_mut().find(|(k, _)| *k == group) {
                    Some((_, (users, known_roots))) => {
                        if !users.contains(&idx) {
                            users.push(idx);
                        }
                        for root in roots {
                            if !known_roots.contains(&root) {
                                known_roots.push(root);
                            }
                        }
                    }
                    None => shared.push((group, (vec![idx], roots))),
                }
            }
        }
        for (_, (users, roots)) in shared.iter_mut() {
            users.sort();
            roots.sort();
        }
        shared.retain(|(_, (users, _))| users.len() > 1);
        shared.sort_by(|a, b| a.0.cmp(&b.0));

        let at_risk: Vec<AtRisk> = shared
            .into_iter()
            .map(
                |((key_attribute, template), (entities_using, consistency_roots))| {
                    warnings.push(format!(
                        "{} both build {} from template '{}', which reads {} outside \
                     [consistency] fields: if they both appear in this import their keys \
                     will not agree and the entities will not join",
                        entity_list(&entities, &entities_using),
                        key_attribute,
                        template,
                        quoted_list(&consistency_roots)
                    ));
                    AtRisk {
                        key_attribute,
                        template,
                        consistency_roots,
                        entities: entities_using,
                        seen_keys: std::collections::HashMap::new(),
                        diverged: HashSet::new(),
                        capped: false,
                    }
                },
            )
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
                warned_mismatch: HashSet::new(),
                warned_unrenderable: HashSet::new(),
                warned_rule_wins: HashSet::new(),
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
    pub fn plan(&mut self, item: &Item, warnings: &mut Vec<String>) -> Option<Rederivation> {
        let Some(entity_idx) = self.resolve_entity(item) else {
            self.unmatched += 1;
            return None;
        };
        let at_risk_originals = self.at_risk_originals(entity_idx, item);

        let entity = &self.entities[entity_idx];

        let mut keys = Vec::new();
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
            } else if self.warned_mismatch.insert((entity_idx, idx)) {
                warnings.push(format!(
                    "entity '{}': template '{}' does not reproduce {} on at least one item \
                     (the attributes it names are missing, not scalars, or the key was \
                     built differently); such keys are left unchanged",
                    entity.name, key.template, key.attribute
                ));
            }
        }

        Some(Rederivation {
            entity: entity_idx,
            keys,
            at_risk_originals,
        })
    }

    /// The value this item arrived with for each at-risk attribute, hashed.
    fn at_risk_originals(&self, entity_idx: usize, item: &Item) -> Vec<(usize, u64)> {
        self.at_risk
            .iter()
            .enumerate()
            .filter(|(_, risk)| risk.entities.contains(&entity_idx) && !risk.capped)
            .filter_map(|(idx, risk)| Some((idx, scalar_hash(item.get(&risk.key_attribute)?)?)))
            .collect()
    }

    /// After the rules ran: compare what this item's at-risk values became
    /// against what an earlier item of a different entity made of the same
    /// original. Two entities that agree, because the action is
    /// deterministic or because no rule matched, are left alone.
    fn note_at_risk_results(&mut self, plan: &Rederivation, item: &Item) {
        for (risk_idx, original) in &plan.at_risk_originals {
            let risk = &mut self.at_risk[*risk_idx];
            if risk.capped {
                continue;
            }
            let Some(now) = item.get(&risk.key_attribute).and_then(scalar_hash) else {
                continue;
            };

            if !risk.seen_keys.contains_key(original) && risk.seen_keys.len() >= MAX_TRACKED_KEYS {
                risk.capped = true;
                continue;
            }
            let outcomes = risk.seen_keys.entry(*original).or_default();

            // Every distinct outcome is kept, not just the first, or whether
            // a break is noticed would depend on the order the export happens
            // to be in.
            let mut diverged_with: Vec<usize> = outcomes
                .iter()
                .filter(|(entity, result)| *entity != plan.entity && *result != now)
                .map(|(entity, _)| *entity)
                .collect();
            if !outcomes.contains(&(plan.entity, now)) {
                outcomes.push((plan.entity, now));
            }
            if !diverged_with.is_empty() {
                diverged_with.push(plan.entity);
                for entity in diverged_with {
                    risk.diverged.insert(entity);
                }
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
        warnings: &mut Vec<String>,
    ) {
        let entity = &self.entities[plan.entity];
        let mut rebuilt_primary = false;
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
                    rebuilt_primary |= Some(key.attribute.as_str())
                        == self.hash_attribute.as_deref()
                        || Some(key.attribute.as_str()) == self.range_attribute.as_deref();
                }
                None => {
                    if self.warned_unrenderable.insert((plan.entity, idx)) {
                        warnings.push(format!(
                            "entity '{}': cannot rebuild {} from template '{}' after the rules \
                             ran (an attribute it needs is no longer a string or number); \
                             the original key value is left in place",
                            entity.name, key.attribute, key.template
                        ));
                    }
                }
            }
        }
        for idx in rule_wins {
            if self.warned_rule_wins.insert((plan.entity, idx)) {
                let key = &self.entities[plan.entity].keys[idx];
                warnings.push(format!(
                    "entity '{}': a rule rewrote {} directly, so it was not rebuilt from \
                     template '{}'",
                    self.entities[plan.entity].name, key.attribute, key.template
                ));
            }
        }
        if rebuilt_primary {
            self.note_rebuilt_primary_key(item);
        }
        self.note_at_risk_results(plan, item);
    }

    /// Remember a rebuilt primary key so a later item rendering the same one
    /// is counted as a collision.
    fn note_rebuilt_primary_key(&mut self, item: &Item) {
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
                    n.hash(&mut hasher);
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
            .filter(|risk| risk.diverged.len() > 1)
            .map(|risk| {
                let mut seen: Vec<usize> = risk.diverged.iter().copied().collect();
                seen.sort();

                format!(
                    "{} took the same original {} to different values, so their keys no longer \
                     agree and they will not join. Template '{}' reads {}: add {} to \
                     [consistency] fields",
                    entity_list(&self.entities, &seen),
                    risk.key_attribute,
                    risk.template,
                    quoted_list(&risk.consistency_roots),
                    quoted_list(&risk.consistency_roots)
                )
            })
            .collect()
    }

    /// Number of items since the last call that matched no entity.
    pub fn take_unmatched(&mut self) -> usize {
        std::mem::take(&mut self.unmatched)
    }

    /// Rebuilt items whose primary key repeated an earlier rebuilt key, and
    /// whether the check stopped early because the table was too large to
    /// track in full.
    pub fn take_collisions(&mut self) -> (usize, bool) {
        (
            std::mem::take(&mut self.collisions),
            self.collision_check_capped,
        )
    }

    /// Find the item's entity: by its type attribute first, otherwise the
    /// first entity whose templates reproduce every key the item carries.
    fn resolve_entity(&self, item: &Item) -> Option<usize> {
        for type_attribute in &self.type_attributes {
            let Some(AttributeValue::S(type_value)) = item.get(type_attribute) else {
                continue;
            };
            let by_type = self
                .entities
                .iter()
                .position(|e| e.name == *type_value && e.type_attribute == *type_attribute);
            if by_type.is_some() {
                return by_type;
            }
        }

        self.entities.iter().position(|e| {
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
            all_match && present > 0
        })
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
fn scalar_hash(value: &AttributeValue) -> Option<u64> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match value {
        AttributeValue::S(s) => s.hash(&mut hasher),
        AttributeValue::N(n) => n.hash(&mut hasher),
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

    fn key_schema(name: &str, kind: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type: kind,
        }
    }

    fn rule(path: &str, action: ValidatedAction) -> ValidatedRule {
        ValidatedRule {
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
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("targets key attribute 'sk' directly"));

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
        assert!(warnings[0].contains("collapse onto one row"));
        assert!(warnings[0].contains("'email'"));

        let rules = [rule(
            "email",
            ValidatedAction::Fake {
                generator: "safe_email".into(),
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
    fn unrelated_pairs_reading_the_same_attribute_name_are_separate_risks() {
        // Account/Team share account#${id}; Project/Task share project#${id}.
        // Same source name, two unrelated groups, so importing one from each
        // must not read as a broken join between them.
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
            },
        )];
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();
        assert_eq!(warnings.len(), 2, "one per group: {warnings:?}");

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
            },
        )];
        let (mut d, warnings) =
            KeyDeriver::new(&model, &request(), &rules, &no_consistency()).unwrap();
        assert!(
            warnings.iter().any(|w| w.contains("${contact.email}")),
            "the template should be shown: {warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("reads 'contact'")),
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
            breaks[0].contains("add 'contact' to [consistency] fields"),
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
        assert!(breaks[0].contains("add 'email' to [consistency] fields"));
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
