//! How the library reads the base64 text of a `B` value when it decodes a
//! value from DynamoDB JSON. The wire tests in `binary_base64_values.rs` hold
//! the captured rules (eu-west-2, 2026-11-11); the same decoder reads every
//! value, so an unpadded value reads here as it does on the wire.

use dynoxide::types::AttributeValue;
use serde_json::json;

fn decode(value: serde_json::Value) -> Result<AttributeValue, String> {
    serde_json::from_value::<AttributeValue>(value).map_err(|e| e.to_string())
}

#[test]
fn an_unpadded_value_reads_as_its_bytes() {
    assert_eq!(
        serde_json::from_str::<AttributeValue>(r#"{"B":"QQ"}"#).unwrap(),
        AttributeValue::B(vec![0x41])
    );
    assert_eq!(
        decode(json!({"BS": ["QUI", "QQ=="]})),
        Ok(AttributeValue::BS(vec![vec![0x41, 0x42], vec![0x41]]))
    );
    assert_eq!(
        decode(json!({"L": [{"B": "-Q=="}]})),
        Ok(AttributeValue::L(vec![AttributeValue::B(vec![0xFD])]))
    );
}

#[test]
fn a_value_dynamodb_refuses_is_refused_in_its_words() {
    let refused = decode(json!({"B": "!!"})).unwrap_err();
    assert!(
        refused.contains("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
        "{refused}"
    );
}
