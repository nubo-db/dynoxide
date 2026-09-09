//! Anonymisation rule engine.
//!
//! Applies anonymisation rules to DynamoDB items: fake data generation,
//! masking, hashing, redaction, and null replacement.

use crate::expressions::{resolve_path, set_path};
use crate::types::{AttributeValue, Item};

use super::config::{Salt, ValidatedAction, ValidatedRule, matches_item};
use super::consistency::ConsistencyMap;

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

/// Apply all matching rules to an item, mutating it in place.
///
/// Returns a list of warnings (e.g., key attribute collision risks).
pub fn apply_rules(
    item: &mut Item,
    rules: &[ValidatedRule],
    consistency_map: &mut ConsistencyMap,
    consistency_fields: &std::collections::HashSet<String>,
    key_attrs: &[String],
) -> Vec<String> {
    let mut warnings = Vec::new();

    for rule in rules {
        if !matches_item(rule, item) {
            continue;
        }

        // Resolve the current value at the path
        let current_value = resolve_path(item, &rule.path);
        if current_value.is_none() {
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
        let is_deterministic = matches!(
            rule.action,
            ValidatedAction::Hash { .. } | ValidatedAction::Fake { seed: Some(_), .. }
        );
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

        // Warn if targeting a key attribute
        if key_attrs.contains(&field_name) {
            warnings.push(format!(
                "anonymising key attribute '{}': potential for collisions",
                field_name
            ));
        }

        // Apply the new value
        if let Err(e) = set_path(item, &rule.path, new_value) {
            warnings.push(format!("failed to set path '{}': {e}", field_name));
        }
    }

    warnings
}

/// Extract the top-level field name from a path.
fn path_to_field_name(path: &[crate::expressions::PathElement]) -> String {
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
            // For numbers, generate a random number string
            let n: u32 = (1000..9999).fake_with_rng(&mut rng);
            AttributeValue::N(n.to_string())
        }
        _ => AttributeValue::S(fake_string),
    }
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
                other => {
                    let json = serde_json::to_string(other).unwrap_or_default();
                    field(b'j', json.as_bytes());
                }
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
/// key is built from merges two identities onto one row. Eight hex characters
/// take it past a hundred billion, which is not a guarantee but is far past
/// the point where it happens in practice.
fn widen_email(address: String, rng: &mut StdRng) -> String {
    let discriminator: u32 = rng.r#gen();
    match address.split_once('@') {
        Some((local, domain)) => format!("{local}.{discriminator:08x}@{domain}"),
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

/// Hash a value using SHA-256 with optional salt.
fn hash_value(original: &AttributeValue, salt: &[u8]) -> AttributeValue {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    match original {
        AttributeValue::S(s) => hasher.update(s.as_bytes()),
        AttributeValue::N(n) => hasher.update(n.as_bytes()),
        AttributeValue::B(b) => hasher.update(b),
        _ => {
            let json = serde_json::to_string(original).unwrap_or_default();
            hasher.update(json.as_bytes());
        }
    }
    let hash = hasher.finalize();
    let hex = hex_encode(&hash);

    AttributeValue::S(hex)
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

        // Without length prefixes both of these hash "ab" + "word" + "x".
        assert_ne!(
            derived("word", &AttributeValue::S("x".into()), &seed("ab")),
            derived("bword", &AttributeValue::S("x".into()), &seed("a")),
            "seed and generator boundaries must be unambiguous"
        );

        // Without a type tag these hash the same bytes.
        let s = seed("a-secret");
        assert_ne!(
            derived("word", &AttributeValue::S("123".into()), &s),
            derived("word", &AttributeValue::N("123".into()), &s),
            "the attribute type is part of the input"
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
    fn a_non_address_generator_is_not_mangled_by_widening() {
        let s = seed("a-secret");
        let v = generate_fake("word", &AttributeValue::S("x".to_string()), Some(&s));
        match v {
            AttributeValue::S(w) => assert!(!w.contains('@'), "word should stay a word: {w}"),
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
        matches!(result, AttributeValue::S(_));

        let result = generate_fake("name", &AttributeValue::N("42".to_string()), None);
        matches!(result, AttributeValue::N(_));
    }
}
