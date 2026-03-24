use dynoxide::{AttributeValue, ConversionError};
use std::collections::{BTreeSet, HashMap, HashSet};

// ---------------------------------------------------------------------------
// From<T> for AttributeValue
// ---------------------------------------------------------------------------

#[test]
fn from_string() {
    let av: AttributeValue = String::from("hello").into();
    assert_eq!(av, AttributeValue::S("hello".to_string()));
}

#[test]
fn from_str() {
    let av: AttributeValue = "hello".into();
    assert_eq!(av, AttributeValue::S("hello".to_string()));
}

#[test]
fn from_bool_true() {
    let av: AttributeValue = true.into();
    assert_eq!(av, AttributeValue::BOOL(true));
}

#[test]
fn from_bool_false() {
    let av: AttributeValue = false.into();
    assert_eq!(av, AttributeValue::BOOL(false));
}

#[test]
fn from_vec_u8() {
    let av: AttributeValue = vec![1u8, 2, 3].into();
    assert_eq!(av, AttributeValue::B(vec![1, 2, 3]));
}

#[test]
fn from_byte_slice() {
    let bytes: &[u8] = &[0xff, 0x00];
    let av: AttributeValue = bytes.into();
    assert_eq!(av, AttributeValue::B(vec![0xff, 0x00]));
}

#[test]
fn from_i64() {
    let av: AttributeValue = 42i64.into();
    assert_eq!(av, AttributeValue::N("42".to_string()));
}

#[test]
fn from_i32() {
    let av: AttributeValue = (-100i32).into();
    assert_eq!(av, AttributeValue::N("-100".to_string()));
}

#[test]
fn from_u64() {
    let av: AttributeValue = u64::MAX.into();
    assert_eq!(av, AttributeValue::N(u64::MAX.to_string()));
}

#[test]
fn from_i128() {
    let av: AttributeValue = i128::MIN.into();
    assert_eq!(av, AttributeValue::N(i128::MIN.to_string()));
}

#[test]
fn from_u8() {
    let av: AttributeValue = 255u8.into();
    assert_eq!(av, AttributeValue::N("255".to_string()));
}

#[test]
fn from_hashmap() {
    let mut m = HashMap::new();
    m.insert("key".to_string(), AttributeValue::S("val".to_string()));
    let av: AttributeValue = m.clone().into();
    assert_eq!(av, AttributeValue::M(m));
}

#[test]
fn from_vec_attribute_value() {
    let list = vec![AttributeValue::S("a".into()), AttributeValue::N("1".into())];
    let av: AttributeValue = list.clone().into();
    assert_eq!(av, AttributeValue::L(list));
}

#[test]
fn from_hashset_string() {
    let mut set = HashSet::new();
    set.insert("a".to_string());
    set.insert("b".to_string());
    let av: AttributeValue = set.into();
    match av {
        AttributeValue::SS(ref ss) => {
            assert_eq!(ss.len(), 2);
            assert!(ss.contains(&"a".to_string()));
            assert!(ss.contains(&"b".to_string()));
        }
        _ => panic!("expected SS"),
    }
}

#[test]
fn from_btreeset_string() {
    let mut set = BTreeSet::new();
    set.insert("x".to_string());
    set.insert("y".to_string());
    let av: AttributeValue = set.into();
    match av {
        AttributeValue::SS(ref ss) => {
            assert_eq!(ss.len(), 2);
            assert!(ss.contains(&"x".to_string()));
            assert!(ss.contains(&"y".to_string()));
        }
        _ => panic!("expected SS"),
    }
}

// ---------------------------------------------------------------------------
// TryFrom<T> for AttributeValue (floats)
// ---------------------------------------------------------------------------

#[test]
fn try_from_f64_valid() {
    let av = AttributeValue::try_from(3.14f64).unwrap();
    assert_eq!(av, AttributeValue::N("3.14".to_string()));
}

#[test]
fn try_from_f64_zero() {
    let av = AttributeValue::try_from(0.0f64).unwrap();
    assert_eq!(av, AttributeValue::N("0".to_string()));
}

#[test]
fn try_from_f64_negative() {
    let av = AttributeValue::try_from(-42.5f64).unwrap();
    assert_eq!(av, AttributeValue::N("-42.5".to_string()));
}

#[test]
fn try_from_f64_nan_fails() {
    let result = AttributeValue::try_from(f64::NAN);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.expected, "finite f64");
}

#[test]
fn try_from_f64_infinity_fails() {
    assert!(AttributeValue::try_from(f64::INFINITY).is_err());
    assert!(AttributeValue::try_from(f64::NEG_INFINITY).is_err());
}

#[test]
fn try_from_f32_valid() {
    let av = AttributeValue::try_from(1.5f32).unwrap();
    assert_eq!(av, AttributeValue::N("1.5".to_string()));
}

#[test]
fn try_from_f32_nan_fails() {
    assert!(AttributeValue::try_from(f32::NAN).is_err());
}

// ---------------------------------------------------------------------------
// TryFrom<AttributeValue> for T (reverse direction)
// ---------------------------------------------------------------------------

#[test]
fn av_to_string() {
    let av = AttributeValue::S("hello".to_string());
    let s: String = av.try_into().unwrap();
    assert_eq!(s, "hello");
}

#[test]
fn av_to_string_wrong_type() {
    let av = AttributeValue::N("42".to_string());
    let result: Result<String, ConversionError> = av.try_into();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().actual, "N");
}

#[test]
fn av_to_bool() {
    let av = AttributeValue::BOOL(true);
    let b: bool = av.try_into().unwrap();
    assert!(b);
}

#[test]
fn av_to_bool_wrong_type() {
    let av = AttributeValue::S("true".to_string());
    let result: Result<bool, ConversionError> = av.try_into();
    assert!(result.is_err());
}

#[test]
fn av_to_bytes() {
    let av = AttributeValue::B(vec![1, 2, 3]);
    let b: Vec<u8> = av.try_into().unwrap();
    assert_eq!(b, vec![1, 2, 3]);
}

#[test]
fn av_to_i64() {
    let av = AttributeValue::N("42".to_string());
    let n: i64 = av.try_into().unwrap();
    assert_eq!(n, 42);
}

#[test]
fn av_to_i64_negative() {
    let av = AttributeValue::N("-100".to_string());
    let n: i64 = av.try_into().unwrap();
    assert_eq!(n, -100);
}

#[test]
fn av_to_i64_parse_failure() {
    let av = AttributeValue::N("not_a_number".to_string());
    let result: Result<i64, ConversionError> = av.try_into();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().actual, "N (parse failed)");
}

#[test]
fn av_to_i64_wrong_type() {
    let av = AttributeValue::S("42".to_string());
    let result: Result<i64, ConversionError> = av.try_into();
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().actual, "S");
}

#[test]
fn av_to_f64() {
    let av = AttributeValue::N("3.14".to_string());
    let n: f64 = av.try_into().unwrap();
    assert!((n - 3.14).abs() < f64::EPSILON);
}

#[test]
fn av_to_u32() {
    let av = AttributeValue::N("1000".to_string());
    let n: u32 = av.try_into().unwrap();
    assert_eq!(n, 1000);
}

#[test]
fn av_to_hashmap() {
    let mut expected = HashMap::new();
    expected.insert("k".to_string(), AttributeValue::S("v".to_string()));
    let av = AttributeValue::M(expected.clone());
    let m: HashMap<String, AttributeValue> = av.try_into().unwrap();
    assert_eq!(m, expected);
}

#[test]
fn av_to_vec_attribute_value() {
    let expected = vec![AttributeValue::S("a".into())];
    let av = AttributeValue::L(expected.clone());
    let l: Vec<AttributeValue> = av.try_into().unwrap();
    assert_eq!(l, expected);
}

#[test]
fn av_to_vec_string_from_ss() {
    let av = AttributeValue::SS(vec!["a".to_string(), "b".to_string()]);
    let v: Vec<String> = av.try_into().unwrap();
    assert_eq!(v, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn av_to_vec_string_from_list_of_s() {
    let av = AttributeValue::L(vec![
        AttributeValue::S("x".to_string()),
        AttributeValue::S("y".to_string()),
    ]);
    let v: Vec<String> = av.try_into().unwrap();
    assert_eq!(v, vec!["x".to_string(), "y".to_string()]);
}

#[test]
fn av_to_vec_string_from_list_with_non_string_fails() {
    let av = AttributeValue::L(vec![
        AttributeValue::S("ok".to_string()),
        AttributeValue::N("42".to_string()),
    ]);
    let result: Result<Vec<String>, ConversionError> = av.try_into();
    assert!(result.is_err());
}

#[test]
fn av_to_vec_string_wrong_type() {
    let av = AttributeValue::N("42".to_string());
    let result: Result<Vec<String>, ConversionError> = av.try_into();
    assert!(result.is_err());
}
