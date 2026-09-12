//! Anonymisation rule engine.
//!
//! Applies anonymisation rules to DynamoDB items: fake data generation,
//! masking, hashing, redaction, and null replacement.

use crate::expressions::{resolve_path, set_path};
use crate::types::{AttributeValue, Item};

use super::config::{Salt, ValidatedAction, ValidatedRule, matches_item};
use super::consistency::ConsistencyMap;

use hmac::{Hmac, Mac};

/// The MAC behind `hash` rules. See [`hash_value`].
type HmacSha256 = Hmac<Sha256>;

use fake::Fake;
use fake::faker::address::en::CityName;
use fake::faker::company::en::CompanyName;
use fake::faker::internet::en::SafeEmail;
use fake::faker::lorem::en::{Sentence, Word};
use fake::faker::name::en::{FirstName, LastName, Name};
use fake::faker::phone_number::en::PhoneNumber;
use fake::rand::rngs::StdRng;
use fake::rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};

/// What one rule did across an import, so the run can report a rule that did
/// nothing rather than let a misspelt path read as a clean pass.
#[derive(Debug, Default, Clone, Copy)]
pub struct RuleWork {
    /// Items whose match expression the rule accepted.
    pub matched: usize,
    /// Matched items that did not carry the rule's path.
    pub path_missing: usize,
    /// Items the rule actually rewrote.
    pub rewrote: usize,
}

/// What `apply_rules` accumulates across every item of a table, so a run can
/// report it once at the end rather than once per item.
pub struct RuleTally<'a> {
    /// Values a `mask` rule left as they arrived, by attribute.
    pub mask_passthroughs: &'a mut std::collections::HashMap<String, usize>,
    /// What each rule did, one entry per rule.
    pub rule_work: &'a mut [RuleWork],
}

/// Apply all matching rules to an item, mutating it in place.
///
/// Returns the warnings raised (e.g. key attribute collision risks) and the
/// top-level attributes actually rewritten. Values a `mask` rule left as they
/// arrived are counted into `mask_passthroughs`, keyed by attribute, since the
/// output alone cannot tell a short value that was skipped from one that was
/// never personal. The caller needs the second to
/// know which keys a rule has taken over: predicting it from the rules is
/// wrong, because each rule's condition sees the item as the rules before it
/// left it, not as it arrived.
pub fn apply_rules(
    item: &mut Item,
    table: &str,
    rules: &[ValidatedRule],
    consistency_map: &mut ConsistencyMap,
    consistency_fields: &std::collections::HashSet<String>,
    key_attrs: &[String],
    tally: &mut RuleTally<'_>,
) -> (Vec<String>, std::collections::HashSet<String>) {
    let RuleTally {
        mask_passthroughs,
        rule_work,
    } = tally;
    let mut warnings = Vec::new();
    let mut rewritten = std::collections::HashSet::new();
    // Attributes a mask left whole and nothing since has rewritten. Judged
    // once the rules have all run: a later rule that replaces the value has
    // removed it, and reporting the mask's pass-through then would say real
    // data survived a run that removed it.
    let mut kept_whole_fields: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (rule_idx, rule) in rules.iter().enumerate() {
        // What each rule actually did, so a run can say when one did nothing.
        // A rules file is the operator's statement of what is sensitive here;
        // a rule that never fires means that statement was not carried out,
        // and without this the run looks identical to one that worked.
        let work = rule_work
            .get_mut(rule_idx)
            .expect("one entry per rule, sized by the caller");
        // A rule scoped to other tables neither matches nor counts here.
        if !rule.applies_to(table) {
            continue;
        }
        if !matches_item(rule, item) {
            continue;
        }
        work.matched += 1;

        // Resolve the current value at the path
        let current_value = resolve_path(item, &rule.path);
        if current_value.is_none() {
            work.path_missing += 1;
            continue; // Path doesn't exist in this item, skip
        }
        let current_value = current_value.unwrap();

        // Determine the field name for consistency tracking
        let field_name = path_to_field_name(&rule.path);
        let is_consistency_field = consistency_fields.contains(&field_name);

        // Generate the anonymised value.
        // Hash actions are deterministic (same input → same output), so they
        // don't need the consistency map: skip it entirely to avoid unbounded
        // memory growth on high-cardinality fields.
        // A seeded fake is a pure function of the input, like hash, so the
        // consistency map would only duplicate what the derivation already
        // guarantees and grow unboundedly doing it.
        // `hash` derives from a canonical encoding, so it is a function of the
        // value for every type. A seeded `fake` only derives from a scalar:
        // `seeded_rng` draws from entropy for a map, list or set rather than
        // claim a repeatability it cannot deliver, so for those the
        // consistency map is the only thing keeping two items agreeing and
        // skipping it would quietly give one input two values.
        let is_deterministic = match &rule.action {
            ValidatedAction::Hash { .. } => true,
            ValidatedAction::Fake { seed: Some(_), .. } => is_scalar(&current_value),
            _ => false,
        };
        let new_value = if is_consistency_field && !is_deterministic {
            // Check consistency map first
            if let Some(cached) = consistency_map.get(&field_name, &current_value) {
                cached
            } else {
                let generated = generate_value(&rule.action, &current_value);
                if let Some(cap_warning) = consistency_map.insert(
                    field_name.clone(),
                    current_value.clone(),
                    generated.clone(),
                ) {
                    warnings.push(cap_warning);
                }
                generated
            }
        } else {
            generate_value(&rule.action, &current_value)
        };

        // A mask keeps the last few characters, so a value no longer than that
        // comes back whole. Nothing in the output says so: the column looks
        // masked because most of it is, and the short rows are the ones most
        // likely to be a code or an initial that still identifies someone.
        //
        // Read the condition rather than compare the values. A long value that
        // already begins with the mask character masks to itself and would
        // read as short, and two mask rules on one attribute would each count
        // the same item once, so the count is per item and attribute.
        let kept_whole = match &rule.action {
            ValidatedAction::Mask { keep_last, .. } => match &current_value {
                AttributeValue::S(s) => s.chars().count() <= *keep_last,
                AttributeValue::N(n) => n.len() <= *keep_last,
                // Every other type is replaced wholesale with mask characters
                // rather than returned, so nothing of it is kept. Counting
                // those would report that real data survived a rule that had
                // in fact removed all of it, which is the wrong direction for
                // a warning whose whole purpose is to say what got through.
                _ => false,
            },
            _ => false,
        };

        // Warn if targeting a key attribute
        if key_attrs.contains(&field_name) {
            warnings.push(format!(
                "anonymising key attribute '{}': potential for collisions",
                field_name
            ));
        }

        // Apply the new value
        match set_path(item, &rule.path, new_value) {
            Ok(()) => {
                rule_work
                    .get_mut(rule_idx)
                    .expect("one entry per rule")
                    .rewrote += 1;
                if kept_whole {
                    kept_whole_fields.insert(field_name.clone());
                } else {
                    kept_whole_fields.remove(&field_name);
                }
                rewritten.insert(field_name);
            }
            Err(e) => {
                warnings.push(format!("failed to set path '{}': {e}", field_name));
            }
        }
    }

    for field in kept_whole_fields {
        *mask_passthroughs.entry(field).or_insert(0) += 1;
    }

    (warnings, rewritten)
}

/// Extract the top-level field name from a path.
pub(super) fn path_to_field_name(path: &[crate::expressions::PathElement]) -> String {
    match path.first() {
        Some(crate::expressions::PathElement::Attribute(name)) => name.clone(),
        _ => String::new(),
    }
}

/// Generate an anonymised value based on the action type.
fn generate_value(action: &ValidatedAction, original: &AttributeValue) -> AttributeValue {
    match action {
        ValidatedAction::Fake { generator, seed } => {
            generate_fake(generator, original, seed.as_ref())
        }
        ValidatedAction::Mask {
            keep_last,
            mask_char,
        } => mask_value(original, *keep_last, *mask_char),
        ValidatedAction::Hash { salt } => hash_value(original, salt.as_bytes()),
        ValidatedAction::Redact => redact_value(original),
        ValidatedAction::Null => AttributeValue::NULL(true),
    }
}

/// Generate fake data based on the generator name.
///
/// With a seed the value is a function of the original, so the same input
/// gives the same output on every run and a committed fixture can be
/// refreshed without churn. Without one each call re-rolls, which is the
/// original behaviour and why the consistency map exists.
fn generate_fake(
    generator: &str,
    original: &AttributeValue,
    seed: Option<&Salt>,
) -> AttributeValue {
    let mut rng = seeded_rng(generator, original, seed);

    // Generate a fake string value
    let fake_string: String = match generator {
        "safe_email" => widen_email(SafeEmail().fake_with_rng(&mut rng), &mut rng),
        "name" => Name().fake_with_rng(&mut rng),
        "first_name" => FirstName().fake_with_rng(&mut rng),
        "last_name" => LastName().fake_with_rng(&mut rng),
        "phone_number" => PhoneNumber().fake_with_rng(&mut rng),
        "address" => CityName().fake_with_rng(&mut rng), // Simplified to city name
        "company_name" => CompanyName().fake_with_rng(&mut rng),
        "sentence" => Sentence(3..8).fake_with_rng(&mut rng),
        "word" => Word().fake_with_rng(&mut rng),
        _ => format!("[FAKE:{generator}]"),
    };

    // Preserve the original type
    match original {
        AttributeValue::S(_) => AttributeValue::S(fake_string),
        AttributeValue::N(_) => {
            // Four digits is 8,999 values, so a few hundred items already
            // repeat and each repeat merges two identities onto one key. The
            // email path was widened for exactly this reason; a number needs
            // the same room. Magnitude is not preserved either way, since the
            // draw already replaced it.
            let n: u64 = rng.r#gen();
            AttributeValue::N(n.to_string())
        }
        _ => AttributeValue::S(fake_string),
    }
}

/// Whether a value is one `seeded_rng` can derive from.
///
/// A map, list or set is not: it has no single byte order, so a derivation over
/// it would not be a function of the value. See [`canonical_bytes`], which
/// gives one to the surfaces that need it.
fn is_scalar(value: &AttributeValue) -> bool {
    matches!(
        value,
        AttributeValue::S(_) | AttributeValue::N(_) | AttributeValue::B(_)
    )
}

/// An RNG for one generated value.
///
/// Seeded from the original value when a secret is configured, so generation
/// is a pure function of what went in. The generator name is mixed in too, so
/// two rules over the same attribute do not produce the same draw.
fn seeded_rng(generator: &str, original: &AttributeValue, seed: Option<&Salt>) -> StdRng {
    match seed {
        Some(seed) => {
            let mut hasher = Sha256::new();
            // Length-prefixed, and the value's type tagged. Concatenating the
            // three fields directly would leave their boundaries ambiguous, so
            // a seed of "ab" with generator "c" would derive the same value as
            // a seed of "a" with generator "bc", and the string "123" would
            // derive the same value as the number 123. Two different
            // configurations sharing a mapping is not something the output
            // would ever show you.
            let mut field = |tag: u8, bytes: &[u8]| {
                hasher.update([tag]);
                hasher.update((bytes.len() as u64).to_be_bytes());
                hasher.update(bytes);
            };
            field(b'k', seed.as_bytes());
            field(b'g', generator.as_bytes());
            match original {
                AttributeValue::S(s) => field(b's', s.as_bytes()),
                AttributeValue::N(n) => field(b'n', n.as_bytes()),
                AttributeValue::B(b) => field(b'b', b),
                // Anything else is a map, a list or a set, and their
                // serialised bytes are not stable: `AttributeValue::M` holds a
                // HashMap, so one value can serialise two ways and derive two
                // different pseudonyms. Falling back to entropy is honest
                // about that, where hashing an unstable encoding would claim a
                // determinism it does not have.
                _ => return StdRng::from_entropy(),
            }
            let digest = hasher.finalize();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&digest);
            StdRng::from_seed(bytes)
        }
        None => StdRng::from_entropy(),
    }
}

/// Put enough room in the local part that two people do not land on one
/// address.
///
/// `SafeEmail` draws a first name from about three thousand, across three
/// `example.` domains, so roughly nine thousand values in total. That is small
/// enough that a few hundred items collide, and a collision on an attribute a
/// key is built from merges two identities onto one row.
///
/// Sixteen hex characters take the space to roughly 1.7e23. That is a
/// probabilistic bound, not a guarantee: a 32-bit suffix would still give
/// about a one in a hundred chance of some duplicate across a million
/// distinct inputs, which is an ordinary export size. The importer's collision
/// counter stays as the backstop either way, because a merged identity is
/// silent.
fn widen_email(address: String, rng: &mut StdRng) -> String {
    let discriminator: u64 = rng.r#gen();
    match address.split_once('@') {
        Some((local, domain)) => format!("{local}.{discriminator:016x}@{domain}"),
        // Not an address after all; leave it rather than corrupt it.
        None => address,
    }
}

/// Mask a value, keeping the last N characters visible.
///
/// Uses character (not byte) counting to correctly handle multibyte UTF-8.
fn mask_value(original: &AttributeValue, keep_last: usize, mask_char: char) -> AttributeValue {
    match original {
        AttributeValue::S(s) => {
            let char_count = s.chars().count();
            if char_count <= keep_last {
                AttributeValue::S(s.clone())
            } else {
                let masked_len = char_count - keep_last;
                // Find the byte offset where the last `keep_last` characters start
                let byte_offset = s
                    .char_indices()
                    .nth(masked_len)
                    .map(|(i, _)| i)
                    .unwrap_or(s.len());
                let suffix = &s[byte_offset..];
                let mut masked =
                    String::with_capacity(masked_len * mask_char.len_utf8() + suffix.len());
                for _ in 0..masked_len {
                    masked.push(mask_char);
                }
                masked.push_str(suffix);
                AttributeValue::S(masked)
            }
        }
        AttributeValue::N(n) => {
            // Numbers are ASCII-only, so byte and char counts are identical
            let len = n.len();
            if len <= keep_last {
                AttributeValue::N(n.clone())
            } else {
                let masked_len = len - keep_last;
                let masked: String =
                    mask_char.to_string().repeat(masked_len) + &n[len - keep_last..];
                AttributeValue::S(masked) // Masked numbers become strings
            }
        }
        _ => AttributeValue::S(format!("{mask_char}{mask_char}{mask_char}{mask_char}")),
    }
}

/// Pseudonymise a value with HMAC-SHA256 keyed on the salt.
///
/// HMAC rather than `SHA256(salt || value)`, on the same reasoning as
/// [`seeded_rng`]. A plain prefix leaves the salt and the value sharing one
/// byte string with no boundary, so a salt of `ab` over a value `cd` derives
/// what a salt of `a` derives over `bcd`, and the construction inherits
/// SHA-256's length extension. Keying the salt keeps the two apart. The
/// value is tagged and length-prefixed for the same reason the seed path
/// does it: without a tag the string `123` and the number `123` pseudonymise
/// to one value, and nothing in the output would ever show you that two
/// attributes had been merged.
fn hash_value(original: &AttributeValue, salt: &[u8]) -> AttributeValue {
    let mut mac =
        HmacSha256::new_from_slice(salt).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(&canonical_bytes(original));
    let hex = hex_encode(&mac.finalize().into_bytes());

    AttributeValue::S(hex)
}

/// Encode an attribute value as a canonical, type-tagged, length-prefixed byte
/// string. Two values encode alike exactly when they are the same value.
///
/// Every field carries its tag and its length, so the boundary between two
/// fields can never be read as part of either, and the string `"123"` never
/// encodes as the number `123`.
///
/// Order is the part that needs care. `AttributeValue::M` holds a `HashMap`,
/// whose iteration order differs between two instances of the same map, so
/// encoding one by serialising it directly gives a single value two encodings,
/// and anything derived from those bytes stops being a function of the value.
/// Map entries are sorted by key and set members are sorted before they are
/// written. A list keeps the order it arrived in, because there the order is
/// part of the value rather than an artefact of the container.
pub(super) fn canonical_bytes(value: &AttributeValue) -> Vec<u8> {
    let mut out = Vec::new();
    absorb(&mut out, value);
    out
}

fn absorb(out: &mut Vec<u8>, value: &AttributeValue) {
    fn field(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
        out.push(tag);
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.extend_from_slice(bytes);
    }
    fn header(out: &mut Vec<u8>, tag: u8, members: usize) {
        out.push(tag);
        out.extend_from_slice(&(members as u64).to_be_bytes());
    }

    match value {
        AttributeValue::S(s) => field(out, b's', s.as_bytes()),
        // DynamoDB stores 1, 1.0 and 0.1e1 as one number, and the engine
        // normalises on write, so encoding the spelling the export happened to
        // use would give one value two pseudonyms and break the join between
        // a row that wrote it one way and a row that wrote it the other.
        //
        // Only a value DynamoDB would actually accept, though. The import
        // parser does not validate an `N`, and normalising maps everything it
        // cannot read to "0", so `abc`, `NaN` and an empty string would all
        // encode as zero and three people would share one pseudonym. Anything
        // that is not a number keeps its own bytes under its own tag, which is
        // both honest and injective.
        AttributeValue::N(n) => match crate::types::validate_dynamo_number(n) {
            Ok(()) => field(
                out,
                b'n',
                crate::types::normalize_dynamo_number(n).as_bytes(),
            ),
            Err(_) => field(out, b'x', n.as_bytes()),
        },
        AttributeValue::B(b) => field(out, b'b', b),
        AttributeValue::BOOL(b) => field(out, b'o', &[u8::from(*b)]),
        AttributeValue::NULL(n) => field(out, b'z', &[u8::from(*n)]),
        AttributeValue::SS(members) => {
            let mut sorted: Vec<&String> = members.iter().collect();
            sorted.sort();
            sorted.dedup();
            header(out, b'P', sorted.len());
            for member in sorted {
                field(out, b's', member.as_bytes());
            }
        }
        AttributeValue::NS(members) => {
            let mut sorted: Vec<String> = members
                .iter()
                .map(|m| match crate::types::validate_dynamo_number(m) {
                    Ok(()) => crate::types::normalize_dynamo_number(m),
                    Err(_) => m.clone(),
                })
                .collect();
            sorted.sort();
            sorted.dedup();
            header(out, b'Q', sorted.len());
            for member in &sorted {
                field(out, b'n', member.as_bytes());
            }
        }
        AttributeValue::BS(members) => {
            let mut sorted: Vec<&Vec<u8>> = members.iter().collect();
            sorted.sort();
            sorted.dedup();
            header(out, b'R', sorted.len());
            for member in sorted {
                field(out, b'b', member);
            }
        }
        AttributeValue::L(members) => {
            header(out, b'l', members.len());
            for member in members {
                absorb(out, member);
            }
        }
        AttributeValue::M(entries) => {
            let mut sorted: Vec<(&String, &AttributeValue)> = entries.iter().collect();
            sorted.sort_by(|a, b| a.0.cmp(b.0));
            header(out, b'm', sorted.len());
            for (name, member) in sorted {
                field(out, b'k', name.as_bytes());
                absorb(out, member);
            }
        }
    }
}

/// Simple hex encoding (avoids pulling in the `hex` crate).
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Replace with a redacted placeholder.
fn redact_value(original: &AttributeValue) -> AttributeValue {
    match original {
        AttributeValue::S(_) => AttributeValue::S("[REDACTED]".to_string()),
        AttributeValue::N(_) => AttributeValue::S("[REDACTED]".to_string()),
        AttributeValue::B(_) => AttributeValue::B(Vec::new()),
        AttributeValue::L(_) => AttributeValue::L(Vec::new()),
        AttributeValue::M(_) => AttributeValue::M(std::collections::HashMap::new()),
        _ => AttributeValue::S("[REDACTED]".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_value() {
        let result = mask_value(&AttributeValue::S("1234567890".to_string()), 4, '*');
        assert_eq!(result, AttributeValue::S("******7890".to_string()));
    }

    #[test]
    fn test_mask_value_multibyte_utf8() {
        // "héllo" has 5 characters but 6 bytes (é is 2 bytes)
        let result = mask_value(&AttributeValue::S("héllo".to_string()), 2, '*');
        assert_eq!(result, AttributeValue::S("***lo".to_string()));

        // Japanese: 3 characters, 9 bytes
        let result = mask_value(&AttributeValue::S("日本語".to_string()), 1, '*');
        assert_eq!(result, AttributeValue::S("**語".to_string()));
    }

    #[test]
    fn test_mask_short_value() {
        let result = mask_value(&AttributeValue::S("ab".to_string()), 4, '*');
        assert_eq!(result, AttributeValue::S("ab".to_string()));
    }

    #[test]
    fn test_hash_value_deterministic() {
        let salt = b"test_salt";
        let v1 = hash_value(&AttributeValue::S("hello".to_string()), salt);
        let v2 = hash_value(&AttributeValue::S("hello".to_string()), salt);
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_hash_value_different_with_different_salt() {
        let v1 = hash_value(&AttributeValue::S("hello".to_string()), b"salt1");
        let v2 = hash_value(&AttributeValue::S("hello".to_string()), b"salt2");
        assert_ne!(v1, v2);
    }

    #[test]
    fn test_salt_and_value_do_not_share_a_boundary() {
        // Under SHA256(salt || value) these two hash the same bytes, so one
        // salt's output is another salt's output over a different value.
        // Keying the salt is what separates them.
        let shifted = hash_value(&AttributeValue::S("cd".to_string()), b"ab");
        let other = hash_value(&AttributeValue::S("bcd".to_string()), b"a");
        assert_ne!(
            shifted, other,
            "the salt must not run into the value as one byte string"
        );
    }

    /// A rule over `path` whose condition matches every item.
    fn test_rule(path: &str, action: ValidatedAction) -> ValidatedRule {
        ValidatedRule {
            condition: crate::expressions::condition::parse(
                "attribute_not_exists(__no_item_has_this__)",
            )
            .expect("a condition true of every item"),
            names: None,
            values: None,
            path: crate::import::config::parse_path(path).expect("a valid path"),
            action,
            tables: None,
        }
    }

    fn item_with_email() -> Item {
        let mut m = Item::new();
        m.insert(
            "email".to_string(),
            AttributeValue::S("real@example.com".to_string()),
        );
        m
    }

    fn work_for(rules: &[ValidatedRule]) -> Vec<RuleWork> {
        vec![RuleWork::default(); rules.len()]
    }

    fn apply_for_test(
        item: &mut Item,
        rules: &[ValidatedRule],
        work: &mut [RuleWork],
        passthroughs: &mut std::collections::HashMap<String, usize>,
    ) {
        apply_rules(
            item,
            "Users",
            rules,
            &mut ConsistencyMap::new(),
            &std::collections::HashSet::new(),
            &[],
            &mut RuleTally {
                mask_passthroughs: passthroughs,
                rule_work: work,
            },
        );
    }

    #[test]
    fn a_rule_scoped_to_another_table_neither_matches_nor_counts() {
        let mut rule = test_rule("email", ValidatedAction::Redact);
        rule.tables = Some(vec!["Orders".to_string()]);
        let rules = [rule];
        let mut work = work_for(&rules);
        let mut passthroughs = std::collections::HashMap::new();
        let mut it = item_with_email();
        apply_for_test(&mut it, &rules, &mut work, &mut passthroughs);

        assert_eq!(work[0].matched, 0, "the rule is not for this table");
        assert_eq!(
            it.get("email"),
            Some(&AttributeValue::S("real@example.com".to_string())),
            "so it changed nothing here"
        );
    }

    #[test]
    fn a_rule_whose_path_is_absent_records_no_work() {
        // The shape a misspelt path takes. Without a count the run prints a
        // full item total, raises nothing, and exits 0 having anonymised
        // nothing at all.
        let rules = [test_rule("emial", ValidatedAction::Redact)];
        let mut work = work_for(&rules);
        let mut passthroughs = std::collections::HashMap::new();
        let mut it = item_with_email();
        apply_for_test(&mut it, &rules, &mut work, &mut passthroughs);

        assert_eq!(work[0].matched, 1, "the rule still matched the item");
        assert_eq!(work[0].path_missing, 1, "but the attribute is not there");
        assert_eq!(work[0].rewrote, 0, "so nothing was rewritten");
        assert_eq!(
            it.get("email"),
            Some(&AttributeValue::S("real@example.com".to_string())),
            "the real value is untouched, which is the whole problem"
        );
    }

    #[test]
    fn a_rule_that_fires_records_the_work_it_did() {
        // The other direction: the counter has to stay quiet on a rule that
        // worked, or every run would carry the warning and it would be read
        // as noise.
        let rules = [test_rule("email", ValidatedAction::Redact)];
        let mut work = work_for(&rules);
        let mut passthroughs = std::collections::HashMap::new();
        let mut it = item_with_email();
        apply_for_test(&mut it, &rules, &mut work, &mut passthroughs);

        assert_eq!(work[0].matched, 1);
        assert_eq!(work[0].path_missing, 0);
        assert_eq!(work[0].rewrote, 1);
    }

    #[test]
    fn a_mask_that_kept_a_short_value_is_counted_once_per_item() {
        // Two mask rules over one attribute are one item's worth of exposure,
        // not two.
        let short = || {
            let mut m = Item::new();
            m.insert("code".to_string(), AttributeValue::S("ab".to_string()));
            m
        };
        let mask = || ValidatedAction::Mask {
            keep_last: 4,
            mask_char: '*',
        };
        let rules = [test_rule("code", mask()), test_rule("code", mask())];
        let mut work = work_for(&rules);
        let mut passthroughs = std::collections::HashMap::new();
        let mut it = short();
        apply_for_test(&mut it, &rules, &mut work, &mut passthroughs);

        assert_eq!(
            passthroughs.get("code"),
            Some(&1),
            "one item kept its value, however many rules looked at it"
        );
    }

    #[test]
    fn a_value_a_mask_kept_whole_but_a_later_rule_replaced_is_not_a_passthrough() {
        // The mask left "ab" alone; the redact after it did not. Nothing of
        // the original reached the output, so nothing is reported.
        let mut it = Item::new();
        it.insert("code".to_string(), AttributeValue::S("ab".to_string()));
        let rules = [
            test_rule(
                "code",
                ValidatedAction::Mask {
                    keep_last: 4,
                    mask_char: '*',
                },
            ),
            test_rule("code", ValidatedAction::Redact),
        ];
        let mut work = work_for(&rules);
        let mut passthroughs = std::collections::HashMap::new();
        apply_for_test(&mut it, &rules, &mut work, &mut passthroughs);

        assert_eq!(
            it.get("code"),
            Some(&AttributeValue::S("[REDACTED]".to_string())),
            "the later rule won"
        );
        assert!(
            passthroughs.is_empty(),
            "so there is no pass-through to report: {passthroughs:?}"
        );
    }

    #[test]
    fn a_mask_over_a_map_is_not_a_passthrough() {
        // mask replaces a map wholesale with mask characters, so nothing of
        // it survives. Counting it said real data got through when none did.
        let rules = [test_rule(
            "profile",
            ValidatedAction::Mask {
                keep_last: 4,
                mask_char: '*',
            },
        )];
        let mut work = work_for(&rules);
        let mut passthroughs = std::collections::HashMap::new();
        let mut it = Item::new();
        let mut inner = std::collections::HashMap::new();
        inner.insert("a".to_string(), AttributeValue::S("secret".to_string()));
        it.insert("profile".to_string(), AttributeValue::M(inner));
        apply_for_test(&mut it, &rules, &mut work, &mut passthroughs);

        assert_eq!(
            passthroughs.get("profile"),
            None,
            "the map was replaced, so nothing was kept as it arrived"
        );
    }

    #[test]
    fn one_number_has_one_encoding_however_it_is_spelled() {
        // DynamoDB stores 1, 1.0 and 0.1e1 as one number and the engine
        // normalises on write, so two spellings must not take two pseudonyms.
        let salt = b"a-salt-long-enough";
        for other in ["1.0", "01", "0.1e1", "1.00"] {
            assert_eq!(
                hash_value(&AttributeValue::N("1".to_string()), salt),
                hash_value(&AttributeValue::N(other.to_string()), salt),
                "N(\"1\") and N({other:?}) are one number"
            );
        }
        assert_ne!(
            hash_value(&AttributeValue::N("1".to_string()), salt),
            hash_value(&AttributeValue::N("2".to_string()), salt),
            "but two numbers stay two"
        );
    }

    #[test]
    fn a_value_that_is_not_a_number_keeps_its_own_pseudonym() {
        // The import parser does not check that an `N` holds a number, and
        // normalising maps everything unreadable to "0". Encoding through it
        // unguarded put every malformed value on the pseudonym for zero, so
        // three people with three different broken values became one person.
        let salt = b"a-salt-long-enough";
        let zero = hash_value(&AttributeValue::N("0".to_string()), salt);
        for bad in ["abc", "NaN", "", "not-a-number", "1.2.3"] {
            assert_ne!(
                hash_value(&AttributeValue::N(bad.to_string()), salt),
                zero,
                "N({bad:?}) must not land on the pseudonym for zero"
            );
        }
        assert_ne!(
            hash_value(&AttributeValue::N("abc".to_string()), salt),
            hash_value(&AttributeValue::N("NaN".to_string()), salt),
            "and two different broken values stay two"
        );
    }

    #[test]
    fn an_exponent_dynamodb_would_reject_is_not_expanded() {
        // The expansion walks the exponent a character at a time, so an
        // exponent outside DynamoDB's own range turns nine characters of
        // export into gigabytes of string, once per item.
        let started = std::time::Instant::now();
        let out = crate::types::normalize_dynamo_number("1e-2000000000");
        assert!(
            out.len() < 64,
            "an unacceptable exponent must not be expanded: got {} chars",
            out.len()
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "and it must not take a second to decide that"
        );
    }

    #[test]
    fn a_binary_set_is_a_set_too() {
        let salt = b"a-salt-long-enough";
        assert_eq!(
            hash_value(&AttributeValue::BS(vec![vec![1, 2]]), salt),
            hash_value(&AttributeValue::BS(vec![vec![1, 2], vec![1, 2]]), salt)
        );
    }

    #[test]
    fn a_set_is_a_set_whatever_the_export_repeated() {
        // AttributeValue::SS is a Vec, so a hand-written or rewritten export
        // can carry a duplicate DynamoDB would have rejected. One logical set
        // must not take two pseudonyms.
        let salt = b"a-salt-long-enough";
        assert_eq!(
            hash_value(&AttributeValue::SS(vec!["a".into()]), salt),
            hash_value(&AttributeValue::SS(vec!["a".into(), "a".into()]), salt)
        );
        assert_eq!(
            hash_value(&AttributeValue::NS(vec!["1".into()]), salt),
            hash_value(&AttributeValue::NS(vec!["1".into(), "1.0".into()]), salt),
            "and two spellings of one number are one member"
        );
    }

    #[test]
    fn a_generated_number_has_room_not_to_collide() {
        // The four-digit draw this replaced held 8,999 values, so a few
        // hundred items already repeated and each repeat merged two
        // identities onto one key. Counting uniques over a sample that small
        // would pass for the old generator too, so read the width instead.
        let mut seen = std::collections::HashSet::new();
        for n in 0..2000 {
            let original = AttributeValue::N(n.to_string());
            if let AttributeValue::N(drawn) = generate_fake("word", &original, None) {
                seen.insert(drawn);
            }
        }
        assert_eq!(seen.len(), 2000, "2000 draws must not repeat");
        assert!(
            seen.iter().any(|d| d.len() > 10),
            "a u64 draw has to reach past ten digits sometimes: the old \
             four-digit range never could"
        );
    }

    #[test]
    fn test_hash_is_stable_over_a_map() {
        // AttributeValue::M holds a HashMap, and two instances of one value
        // iterate in different orders. Hashing a serialisation of that order
        // gave every item its own pseudonym, so a hashed map attribute joined
        // against nothing, including itself.
        let entries = || {
            let mut m = std::collections::HashMap::new();
            for k in [
                "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
            ] {
                m.insert(k.to_string(), AttributeValue::S(format!("{k}-value")));
            }
            AttributeValue::M(m)
        };
        let salt = b"a-salt-long-enough";
        assert_eq!(
            hash_value(&entries(), salt),
            hash_value(&entries(), salt),
            "one map value must give one pseudonym"
        );
    }

    #[test]
    fn test_hash_is_stable_over_a_nested_map() {
        // The map may be behind a list, where the outer container is ordered
        // and only the inner one is not.
        let nested = || {
            let mut inner = std::collections::HashMap::new();
            for k in ["one", "two", "three", "four", "five", "six"] {
                inner.insert(k.to_string(), AttributeValue::N("1".to_string()));
            }
            AttributeValue::L(vec![
                AttributeValue::S("first".to_string()),
                AttributeValue::M(inner),
            ])
        };
        let salt = b"a-salt-long-enough";
        assert_eq!(hash_value(&nested(), salt), hash_value(&nested(), salt));
    }

    #[test]
    fn test_hash_is_stable_over_a_set_whatever_order_it_arrives_in() {
        // A DynamoDB set is unordered, so two exports of one value can list
        // its members either way round.
        let salt = b"a-salt-long-enough";
        let forwards = AttributeValue::SS(vec!["a".to_string(), "b".to_string()]);
        let backwards = AttributeValue::SS(vec!["b".to_string(), "a".to_string()]);
        assert_eq!(hash_value(&forwards, salt), hash_value(&backwards, salt));
    }

    #[test]
    fn test_a_list_keeps_the_order_it_arrived_in() {
        // Unlike a set, a list's order is part of the value, so two orders are
        // two values and must not share a pseudonym.
        let salt = b"a-salt-long-enough";
        let forwards = AttributeValue::L(vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::S("b".to_string()),
        ]);
        let backwards = AttributeValue::L(vec![
            AttributeValue::S("b".to_string()),
            AttributeValue::S("a".to_string()),
        ]);
        assert_ne!(hash_value(&forwards, salt), hash_value(&backwards, salt));
    }

    #[test]
    fn test_canonical_encoding_keeps_neighbouring_fields_apart() {
        // The same boundary problem the salt had, one level down: without a
        // length on each field, a key "ab" over a value "c" writes the bytes a
        // key "a" over a value "bc" writes.
        let one = |k: &str, v: &str| {
            let mut m = std::collections::HashMap::new();
            m.insert(k.to_string(), AttributeValue::S(v.to_string()));
            AttributeValue::M(m)
        };
        assert_ne!(
            canonical_bytes(&one("ab", "c")),
            canonical_bytes(&one("a", "bc"))
        );
    }

    #[test]
    fn test_containers_of_the_same_members_encode_apart() {
        // A list and a set of one member are different values, and so are an
        // empty map and an empty list.
        let member = || AttributeValue::S("x".to_string());
        assert_ne!(
            canonical_bytes(&AttributeValue::L(vec![member()])),
            canonical_bytes(&AttributeValue::SS(vec!["x".to_string()]))
        );
        assert_ne!(
            canonical_bytes(&AttributeValue::M(std::collections::HashMap::new())),
            canonical_bytes(&AttributeValue::L(Vec::new()))
        );
        assert_ne!(
            canonical_bytes(&AttributeValue::BOOL(false)),
            canonical_bytes(&AttributeValue::NULL(true))
        );
    }

    #[test]
    fn test_hash_over_binary_is_stable_and_distinct() {
        let salt = b"a-salt-long-enough";
        let bytes = AttributeValue::B(vec![0x00, 0xff, 0x10]);
        assert_eq!(hash_value(&bytes, salt), hash_value(&bytes, salt));
        assert_ne!(
            hash_value(&bytes, salt),
            hash_value(&AttributeValue::S("\u{0}\u{ff}\u{10}".to_string()), salt),
            "binary must not land on the string of the same characters"
        );
    }

    #[test]
    fn test_string_and_number_pseudonymise_differently() {
        // Without a type tag `S("123")` and `N("123")` are the same bytes, so
        // two attributes merge onto one pseudonym and nothing in the output
        // says so.
        let s = hash_value(&AttributeValue::S("123".to_string()), b"a-salt-long-enough");
        let n = hash_value(&AttributeValue::N("123".to_string()), b"a-salt-long-enough");
        assert_ne!(s, n, "type must be part of the derivation");
    }

    #[test]
    fn test_hash_value_is_hmac_sha256_of_the_tagged_value() {
        // Pins the construction itself, so a future edit that quietly returns
        // to a prefixed hash fails here rather than silently repseudonymising
        // every hashed column.
        let salt = b"a-salt-long-enough";
        let mut expected = <HmacSha256 as Mac>::new_from_slice(salt).unwrap();
        expected.update(b"s");
        expected.update(&(5u64).to_be_bytes());
        expected.update(b"hello");
        let expected = hex_encode(&expected.finalize().into_bytes());

        assert_eq!(
            hash_value(&AttributeValue::S("hello".to_string()), salt),
            AttributeValue::S(expected)
        );
    }

    #[test]
    fn test_redact_value() {
        assert_eq!(
            redact_value(&AttributeValue::S("secret".to_string())),
            AttributeValue::S("[REDACTED]".to_string())
        );
        assert_eq!(
            redact_value(&AttributeValue::L(vec![AttributeValue::S("a".to_string())])),
            AttributeValue::L(Vec::new())
        );
    }

    fn seed(bytes: &str) -> Salt {
        Salt::new(bytes.as_bytes().to_vec())
    }

    #[test]
    fn seeded_fake_is_a_function_of_the_input() {
        let s = seed("a-secret");
        let alice = AttributeValue::S("alice@example.com".to_string());
        let bob = AttributeValue::S("bob@example.com".to_string());

        // Same input, same seed, same answer, however many times.
        let first = generate_fake("safe_email", &alice, Some(&s));
        for _ in 0..5 {
            assert_eq!(generate_fake("safe_email", &alice, Some(&s)), first);
        }
        // Different input diverges, so identities stay distinct.
        assert_ne!(generate_fake("safe_email", &bob, Some(&s)), first);
        // A different secret gives a different mapping entirely.
        let other = seed("another-secret");
        assert_ne!(generate_fake("safe_email", &alice, Some(&other)), first);
    }

    /// The first draw from the derived stream, which is what every generated
    /// value is a function of.
    fn derived(generator: &str, original: &AttributeValue, s: &Salt) -> u64 {
        seeded_rng(generator, original, Some(s)).r#gen()
    }

    #[test]
    fn the_derivation_separates_its_fields() {
        // Tested against seeded_rng rather than generate_fake on purpose: the
        // generator name also selects the generator, and the original's type
        // also picks the returned variant, so comparing generated values would
        // pass whether or not the derivation kept its fields apart.
        let text = |v: &str| AttributeValue::S(v.into());

        // Length prefixes. With tags but no lengths both of these feed the
        // hasher the identical byte string
        // "k" "a" "g" "words" "s" "x" ... run together.
        assert_ne!(
            derived("word", &text("gsafe_emailsx"), &seed("a")),
            derived("safe_email", &text("x"), &seed("agwords")),
            "field lengths must be part of the input"
        );

        // Type tags. Without them these hash the same bytes.
        let s = seed("a-secret");
        assert_ne!(
            derived("word", &text("123"), &s),
            derived("word", &AttributeValue::N("123".into()), &s),
            "the attribute type is part of the input"
        );

        // The generator itself must reach the hash, not merely select the
        // generator function.
        assert_ne!(
            derived("word", &text("x"), &s),
            derived("safe_email", &text("x"), &s),
            "the generator name is part of the input"
        );
    }

    #[test]
    fn the_email_suffix_is_a_full_64_bits() {
        // A weaker suffix still produces distinct values across a small
        // sample, so counting uniques cannot establish the width. Read the
        // suffix instead: sixteen hex characters, and the high half has to
        // vary too, because a narrower draw zero-extended into the same
        // field would print the same sixteen characters with a constant top.
        let s = seed("a-secret");
        let mut high_halves = std::collections::HashSet::new();
        for n in 0..32 {
            let original = AttributeValue::S(format!("person{n}@b.c"));
            match generate_fake("safe_email", &original, Some(&s)) {
                AttributeValue::S(v) => {
                    let (local, _) = v.split_once('@').expect("an address");
                    let suffix = local.rsplit('.').next().expect("a suffix");
                    assert_eq!(suffix.len(), 16, "expected 16 hex characters in {v}");
                    assert!(
                        suffix.chars().all(|c| c.is_ascii_hexdigit()),
                        "expected hex in {v}"
                    );
                    high_halves.insert(suffix[..8].to_string());
                }
                other => panic!("expected a string, got {other:?}"),
            }
        }
        assert!(
            high_halves.len() > 1,
            "the top 32 bits never varied across 32 inputs: {high_halves:?}"
        );
    }

    #[test]
    fn a_value_whose_bytes_are_not_stable_does_not_claim_determinism() {
        // A map serialises in HashMap order, so hashing it would derive a
        // different pseudonym for the same value on a different run. Better to
        // draw fresh than to promise a stability that is not there.
        let s = seed("a-secret");
        let mut map = std::collections::HashMap::new();
        map.insert("a".to_string(), AttributeValue::S("x".to_string()));
        map.insert("b".to_string(), AttributeValue::S("y".to_string()));
        let value = AttributeValue::M(map);

        let mut seen = std::collections::HashSet::new();
        for _ in 0..20 {
            seen.insert(format!("{:?}", generate_fake("word", &value, Some(&s))));
        }
        assert!(
            seen.len() > 1,
            "a non-scalar should draw fresh rather than pretend to be deterministic"
        );
    }

    #[test]
    fn unseeded_fake_still_re_rolls() {
        let alice = AttributeValue::S("alice@example.com".to_string());
        let mut seen = std::collections::HashSet::new();
        for _ in 0..20 {
            seen.insert(format!("{:?}", generate_fake("safe_email", &alice, None)));
        }
        assert!(seen.len() > 1, "without a seed each call should re-roll");
    }

    #[test]
    fn generated_emails_have_room_to_avoid_collisions() {
        // SafeEmail alone draws from roughly nine thousand values, so a few
        // hundred items collide and merge two identities onto one key. The
        // widened local part is what stops that.
        let s = seed("a-secret");
        let mut seen = std::collections::HashSet::new();
        for n in 0..2_000 {
            let original = AttributeValue::S(format!("person{n}@real.example"));
            match generate_fake("safe_email", &original, Some(&s)) {
                AttributeValue::S(v) => {
                    assert!(v.contains('@'), "still an address: {v}");
                    assert!(seen.insert(v), "collision within 2000 values");
                }
                other => panic!("expected a string, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_non_address_generator_is_left_exactly_as_the_generator_produced_it() {
        // Asserting only "no @ in it" would pass for an empty string or for a
        // word with digits stapled on. Compare against the generator driven by
        // an identically derived stream instead.
        let s = seed("a-secret");
        let original = AttributeValue::S("x".to_string());
        let expected: String = Word().fake_with_rng(&mut seeded_rng("word", &original, Some(&s)));

        match generate_fake("word", &original, Some(&s)) {
            AttributeValue::S(w) => {
                assert_eq!(w, expected, "widening must not touch a non-address")
            }
            other => panic!("expected a string, got {other:?}"),
        }
    }

    #[test]
    fn test_generate_fake_preserves_type() {
        let result = generate_fake(
            "safe_email",
            &AttributeValue::S("old@example.com".to_string()),
            None,
        );
        assert!(matches!(result, AttributeValue::S(_)), "got {result:?}");

        let result = generate_fake("name", &AttributeValue::N("42".to_string()), None);
        assert!(matches!(result, AttributeValue::N(_)), "got {result:?}");
    }
}
