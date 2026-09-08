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

use super::config::{ValidatedAction, ValidatedRule, matches_item, parse_path};

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
    /// Top-level attribute names the template reads.
    fn sources(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|s| match s {
            Segment::Var { path, .. } => match path.first() {
                Some(PathElement::Attribute(name)) => Some(name.as_str()),
                _ => None,
            },
            Segment::Literal(_) => None,
        })
    }
}

/// The templated keys of one entity, resolved against one table's key schema.
#[derive(Debug, Clone)]
struct EntityKeys {
    name: String,
    type_attribute: String,
    keys: Vec<TemplatedKey>,
}

/// An attribute more than one entity builds a key from, which is not in
/// `[consistency] fields`. Anonymising it independently per item means the
/// entities' keys disagree and the join between them is lost.
#[derive(Debug)]
struct AtRisk {
    attribute: String,
    /// Entity indices that build a key from it, in model order.
    entities: Vec<usize>,
    /// Hash of each value seen, against the entity that carried it. A second
    /// entity carrying a value already seen is the join, observed in the data
    /// rather than inferred from the model.
    seen_values: std::collections::HashMap<u64, usize>,
    /// Entities that were found to share a value with another entity.
    sharing: HashSet<usize>,
    /// Set once `seen_values` hit its cap and stopped tracking.
    capped: bool,
}

/// Which of an item's keys can be rebuilt after the rules run.
#[derive(Debug)]
pub struct Rederivation {
    entity: usize,
    keys: Vec<usize>,
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
    /// Key attributes some rule rewrites directly. Whether the rule wins is
    /// decided per item, since its condition may not match every entity that
    /// builds that key.
    rule_targeted: HashSet<String>,
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
        let mut entity_key_shapes: Vec<Vec<(String, String, Vec<String>)>> =
            Vec::with_capacity(model.entities.len());
        for entity in &model.entities {
            let mut keys = Vec::new();
            push_key(&mut keys, entity, hash, Some(&entity.pk_template))?;
            push_key(&mut keys, entity, range, entity.sk_template.as_deref())?;

            for mapping in &entity.gsi_mappings {
                let Some(gsi) = gsis.iter().find(|g| g.index_name == mapping.index_name) else {
                    continue;
                };
                push_key(
                    &mut keys,
                    entity,
                    partition_key_name(&gsi.key_schema),
                    Some(&mapping.pk_template),
                )?;
                push_key(
                    &mut keys,
                    entity,
                    sort_key_name(&gsi.key_schema),
                    mapping.sk_template.as_deref(),
                )?;
            }

            // Recorded before the rule-target retain below: this is a
            // property of the model's templates, not of what survives.
            entity_key_shapes.push(
                keys.iter()
                    .map(|key| {
                        (
                            key.attribute.clone(),
                            key.template.clone(),
                            key.sources().map(String::from).collect(),
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
        let mut shared: Vec<(String, Vec<usize>)> = Vec::new();
        for (idx, shape) in entity_key_shapes.iter().enumerate() {
            for (attribute, template, sources) in shape {
                let also_built_by: Vec<usize> = entity_key_shapes
                    .iter()
                    .enumerate()
                    .filter(|(other, other_shape)| {
                        *other != idx
                            && other_shape
                                .iter()
                                .any(|(a, t, _)| a == attribute && t == template)
                    })
                    .map(|(other, _)| other)
                    .collect();
                if also_built_by.is_empty() {
                    continue;
                }
                for source in sources {
                    // An attribute no rule rewrites keeps its value, so the
                    // two keys still agree however they were built.
                    if consistency_fields.contains(source)
                        || !rule_targets.contains(source.as_str())
                    {
                        continue;
                    }
                    match shared.iter_mut().find(|(name, _)| name == source) {
                        Some((_, users)) => {
                            if !users.contains(&idx) {
                                users.push(idx);
                            }
                        }
                        None => shared.push((source.clone(), vec![idx])),
                    }
                }
            }
        }
        for (_, users) in shared.iter_mut() {
            users.sort();
        }
        shared.retain(|(_, users)| users.len() > 1);
        shared.sort_by(|a, b| a.0.cmp(&b.0));

        let at_risk: Vec<AtRisk> = shared
            .into_iter()
            .map(|(attribute, entities_using)| {
                warnings.push(format!(
                    "{} both build keys from '{}', which is not in [consistency] fields: \
                     if they both appear in this import their keys will not agree and \
                     the entities will not join",
                    entity_list(&entities, &entities_using),
                    attribute
                ));
                AtRisk {
                    attribute,
                    entities: entities_using,
                    seen_values: std::collections::HashMap::new(),
                    sharing: HashSet::new(),
                    capped: false,
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
                rule_targeted: rule_targets.iter().map(|s| s.to_string()).collect(),
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
    pub fn plan(
        &mut self,
        item: &Item,
        rules: &[ValidatedRule],
        warnings: &mut Vec<String>,
    ) -> Option<Rederivation> {
        let Some(entity_idx) = self.resolve_entity(item) else {
            self.unmatched += 1;
            return None;
        };
        self.note_at_risk_values(entity_idx, item);

        let entity = &self.entities[entity_idx];

        let mut rule_wins = Vec::new();
        let mut keys = Vec::new();
        for (idx, key) in entity.keys.iter().enumerate() {
            // A rule that rewrites this key attribute wins over the template,
            // but only on the items its condition actually matches. Deciding
            // that per entity would leave the key unrebuilt on every item the
            // rule never touches, real value intact.
            if self.rule_targeted.contains(&key.attribute)
                && rules.iter().any(|rule| {
                    rule_target(rule) == Some(key.attribute.as_str()) && matches_item(rule, item)
                })
            {
                rule_wins.push(idx);
                continue;
            }

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

        for idx in rule_wins {
            if self.warned_rule_wins.insert((entity_idx, idx)) {
                let key = &self.entities[entity_idx].keys[idx];
                warnings.push(format!(
                    "entity '{}': a rule rewrote {} directly, so it was not rebuilt from \
                     template '{}'",
                    self.entities[entity_idx].name, key.attribute, key.template
                ));
            }
        }

        Some(Rederivation {
            entity: entity_idx,
            keys,
        })
    }

    /// Record this item's values for any at-risk attribute, so a value two
    /// entities both carry is spotted. Presence of both entities is not
    /// enough: two entities keyed on the same template with no value in
    /// common never had a join to lose.
    fn note_at_risk_values(&mut self, entity_idx: usize, item: &Item) {
        for risk in &mut self.at_risk {
            if !risk.entities.contains(&entity_idx) || risk.capped {
                continue;
            }
            let Some(value) = item.get(&risk.attribute) else {
                continue;
            };
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            match value {
                AttributeValue::S(s) => s.hash(&mut hasher),
                AttributeValue::N(n) => n.hash(&mut hasher),
                _ => continue,
            }
            let key = hasher.finish();
            match risk.seen_values.get(&key) {
                Some(previous) if *previous != entity_idx => {
                    risk.sharing.insert(*previous);
                    risk.sharing.insert(entity_idx);
                }
                Some(_) => {}
                None => {
                    if risk.seen_values.len() >= MAX_TRACKED_KEYS {
                        risk.capped = true;
                        continue;
                    }
                    risk.seen_values.insert(key, entity_idx);
                }
            }
        }
    }

    /// After the rules run: render every planned key from the item's current
    /// attributes. A key whose template no longer renders (a rule nulled or
    /// removed an attribute it needs) is left unchanged and reported.
    pub fn apply(&mut self, plan: &Rederivation, item: &mut Item, warnings: &mut Vec<String>) {
        let entity = &self.entities[plan.entity];
        let mut rebuilt_primary = false;
        for &idx in &plan.keys {
            let key = &entity.keys[idx];
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
        if rebuilt_primary {
            self.note_rebuilt_primary_key(item);
        }
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
            if let Some(AttributeValue::S(s)) = item.get(attribute) {
                s.hash(&mut hasher);
            }
            0u8.hash(&mut hasher);
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
            .filter(|risk| risk.sharing.len() > 1)
            .map(|risk| {
                let mut seen: Vec<usize> = risk.sharing.iter().copied().collect();
                seen.sort();
                format!(
                    "{} share a value of '{}', which is not in [consistency] fields, so it \
                     anonymised differently for each and their keys no longer agree. \
                     Add '{}' to [consistency] fields",
                    entity_list(&self.entities, &seen),
                    risk.attribute,
                    risk.attribute
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
            let all_match = e.keys.iter().all(|key| match item.get(&key.attribute) {
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

/// Track `attribute` when a template exists for it and mentions a variable.
fn push_key(
    keys: &mut Vec<TemplatedKey>,
    entity: &EntityDefinition,
    attribute: Option<&str>,
    template: Option<&str>,
) -> Result<(), String> {
    let (Some(attribute), Some(template)) = (attribute, template) else {
        return Ok(());
    };
    let segments = parse_template(template)
        .map_err(|e| format!("entity '{}', {attribute}: {e}", entity.name))?;
    if segments.iter().any(|s| matches!(s, Segment::Var { .. })) {
        keys.push(TemplatedKey {
            attribute: attribute.to_string(),
            template: template.to_string(),
            segments,
        });
    }
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
        let plan = d.plan(&order, &[], &mut item_warnings).unwrap();
        assert!(item_warnings.is_empty(), "{item_warnings:?}");
        assert_eq!(plan.keys.len(), 2);

        order.insert("orderNo".to_string(), AttributeValue::S("7".to_string()));
        d.apply(&plan, &mut order, &mut item_warnings);
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

        let plan = d.plan(&user, &[], &mut warnings).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(plan.keys.len(), 3);

        user.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        d.apply(&plan, &mut user, &mut warnings);

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
            let plan = d.plan(&user, &[], &mut warnings).unwrap();
            assert_eq!(plan.keys.len(), 1, "only pk reproduces");

            user.insert(
                "email".to_string(),
                AttributeValue::S(format!("fake{n}@example.org")),
            );
            d.apply(&plan, &mut user, &mut warnings);
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
        let plan = d.plan(&user, &[], &mut warnings).unwrap();
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
        d.plan(&legacy, &[], &mut warnings).unwrap();
        assert_eq!(warnings.len(), 1);

        // Second item: sk on-template, but the rule nulls email so it cannot render.
        let mut user = user();
        let plan = d.plan(&user, &[], &mut warnings).unwrap();
        user.insert("email".to_string(), AttributeValue::NULL(true));
        d.apply(&plan, &mut user, &mut warnings);

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
        let plan = d.plan(&user, &[], &mut warnings).unwrap();
        assert_eq!(d.entities[plan.entity].name, "User");

        let account = item(&[("pk", "account#acc1"), ("sk", "account#"), ("id", "acc1")]);
        let plan = d.plan(&account, &[], &mut warnings).unwrap();
        assert_eq!(d.entities[plan.entity].name, "Account");

        let stranger = item(&[("pk", "thing#1"), ("sk", "meta")]);
        assert!(d.plan(&stranger, &[], &mut warnings).is_none());
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
        let plan = d.plan(&user, &rules, &mut item_warnings).unwrap();
        user.insert(
            "sk".to_string(),
            AttributeValue::S("[REDACTED]".to_string()),
        );
        d.apply(&plan, &mut user, &mut item_warnings);
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
        let plan = d.plan(&user, &rules, &mut warnings).unwrap();

        user.insert(
            "email".to_string(),
            AttributeValue::S("fake@example.org".to_string()),
        );
        d.apply(&plan, &mut user, &mut warnings);

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
            d.plan(&customer, &email_rule(), &mut warnings).unwrap();
        }

        assert!(
            d.join_breaks().is_empty(),
            "a slice holding one entity has no join to lose: {:?}",
            d.join_breaks()
        );
    }

    #[test]
    fn both_entities_turning_up_is_a_broken_join() {
        let (mut d, _) = KeyDeriver::new(
            &shared_key_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();
        let mut warnings = Vec::new();

        let customer = item(&[
            ("_type", "Customer"),
            ("pk", "CUSTOMER#a@x.co"),
            ("sk", "PROFILE"),
            ("email", "a@x.co"),
        ]);
        d.plan(&customer, &email_rule(), &mut warnings).unwrap();
        assert!(d.join_breaks().is_empty(), "one entity so far");

        let order = item(&[
            ("_type", "Order"),
            ("pk", "CUSTOMER#a@x.co"),
            ("sk", "ORDER#1"),
            ("email", "a@x.co"),
            ("orderId", "1"),
        ]);
        d.plan(&order, &email_rule(), &mut warnings).unwrap();

        let breaks = d.join_breaks();
        assert_eq!(breaks.len(), 1, "{breaks:?}");
        assert!(breaks[0].contains("entity 'Customer' and entity 'Order'"));
        assert!(breaks[0].contains("Add 'email' to [consistency] fields"));
    }

    #[test]
    fn both_entities_with_no_value_in_common_is_not_a_broken_join() {
        let (mut d, _) = KeyDeriver::new(
            &shared_key_model(),
            &request(),
            &email_rule(),
            &no_consistency(),
        )
        .unwrap();
        let mut warnings = Vec::new();

        let customer = item(&[
            ("_type", "Customer"),
            ("pk", "CUSTOMER#a@x.co"),
            ("sk", "PROFILE"),
            ("email", "a@x.co"),
        ]);
        d.plan(&customer, &email_rule(), &mut warnings).unwrap();

        // A different address, so these two never joined.
        let order = item(&[
            ("_type", "Order"),
            ("pk", "CUSTOMER#b@y.co"),
            ("sk", "ORDER#1"),
            ("email", "b@y.co"),
            ("orderId", "1"),
        ]);
        d.plan(&order, &email_rule(), &mut warnings).unwrap();

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
            let plan = d.plan(&user, &[], &mut warnings).unwrap();
            user.insert(
                "email".to_string(),
                AttributeValue::S("[REDACTED]".to_string()),
            );
            d.apply(&plan, &mut user, &mut warnings);
            assert_eq!(user["sk"], AttributeValue::S("user#[REDACTED]".to_string()));
        }
        assert_eq!(d.take_collisions(), (2, false));
        assert_eq!(d.take_collisions(), (0, false));
    }
}
