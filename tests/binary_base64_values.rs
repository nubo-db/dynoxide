//! The base64 text of a `B` value, or a `BS` member, as DynamoDB reads it.
//!
//! DynamoDB has its own base64 reader. It refuses a value one byte long, a
//! length that isn't a multiple of 4 and doesn't pad out to strict base64, and
//! a last character before the padding that leaves bits set, each in its own
//! words; it refuses other faults with no message. It accepts unpadded and
//! part-padded values, and lets characters outside the alphabet through in the
//! final group of a padded value. Every refusal is a `SerializationException`
//! raised as the value is read, ahead of the request's validation.
//!
//! The rows below are the requests sent to eu-west-2 on 11 November 2026 and
//! the answers DynamoDB gave, as data captured from DynamoDB, not written by
//! hand. Most went to a table that doesn't exist, so an answer of
//! `ResourceNotFoundException` means the value was accepted. A message of
//! `None` is an exception with no message.

#![cfg(feature = "http-server")]

use dynoxide::Database;
use dynoxide::actions::create_table::CreateTableRequest;

/// A captured row: its number in the round, the operation, the request body,
/// and DynamoDB's exception and message.
type Row = (
    usize,
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
);

async fn start_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value::<CreateTableRequest>(serde_json::json!({
            "TableName": "Tab",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .unwrap(),
    )
    .unwrap();
    tokio::spawn(async move {
        dynoxide::server::serve_on(listener, db).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    url
}

async fn send(url: &str, operation: &str, body: &str) -> (u16, serde_json::Value) {
    let resp = reqwest::Client::new()
        .post(url)
        .header("x-amz-target", format!("DynamoDB_20120810.{operation}"))
        .header("content-type", "application/x-amz-json-1.0")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=fakekey/20260101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=host;x-amz-date;x-amz-target, Signature=fakesig",
        )
        .header("x-amz-date", "20260101T000000Z")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap())
}

/// The exception type and message a request is refused with, or `None` for
/// one that succeeds. The message of a `ResourceNotFoundException` is left
/// out: it names the table in Dynoxide's words, and all it shows here is that
/// the value was read.
async fn answer(url: &str, operation: &str, body: &str) -> Option<(String, Option<String>)> {
    let (status, json) = send(url, operation, body).await;
    if status == 200 {
        return None;
    }
    let kind = json["__type"]
        .as_str()
        .unwrap()
        .rsplit('#')
        .next()
        .unwrap()
        .to_string();
    let message = json
        .get("Message")
        .or_else(|| json.get("message"))
        .and_then(|m| m.as_str())
        .map(str::to_string)
        .filter(|_| kind != "ResourceNotFoundException");
    Some((kind, message))
}

/// Rows whose answer depends on the order a request is read in, which
/// Dynoxide doesn't follow yet: it looks at a value's members in sorted key
/// order with `BOOL` and `NULL` first, and stops at the first fault it reads
/// rather than parsing the whole request before validating it. A member of the
/// wrong JSON type in the same place has the same gap, so these wait on that.
/// Each is still sent: it must still differ from DynamoDB's answer, and come
/// back as the exception named here, which is Dynoxide's answer today.
const ORDER_NOT_FOLLOWED: &[(&str, usize, &str, &str)] = &[
    (
        "round 1",
        65,
        "a bad number in an attribute written first",
        "ValidationException",
    ),
    (
        "round 1",
        66,
        "an empty value in an attribute written first",
        "ValidationException",
    ),
    (
        "round 1",
        72,
        "a bad BOOL read ahead of the B written first",
        "SerializationException",
    ),
    (
        "round 1",
        76,
        "ReturnValues of the wrong JSON type",
        "SerializationException",
    ),
    (
        "round 5",
        10,
        "a bad BOOL read ahead of the B written first",
        "SerializationException",
    ),
    (
        "round 5",
        11,
        "S of the wrong JSON type written ahead of B",
        "SerializationException",
    ),
    (
        "round 5",
        15,
        "a bad NULL read ahead of the B written first",
        "SerializationException",
    ),
    // The later of the two `B` members is read, and it is valid.
    (
        "round 5",
        17,
        "a duplicate member key",
        "ResourceNotFoundException",
    ),
    (
        "round 5",
        18,
        "a duplicate member key",
        "SerializationException",
    ),
    (
        "round 5",
        27,
        "map values read in key order, not as written",
        "SerializationException",
    ),
];

async fn assert_rows(url: &str, round: &str, rows: &[Row]) {
    let mut wrong = Vec::new();
    let mut compared = 0;
    for &(number, operation, body, kind, message) in rows {
        let expected = Some((kind.to_string(), message.map(str::to_string)));
        let got = answer(url, operation, body).await;
        compared += 1;
        if let Some(&(_, _, reason, today)) = ORDER_NOT_FOLLOWED
            .iter()
            .find(|&&(r, n, _, _)| r == round && n == number)
        {
            if got == expected {
                wrong.push(format!(
                    "{round} row {number} ({reason}) now answers as DynamoDB did: \
                     drop it from ORDER_NOT_FOLLOWED"
                ));
            } else if got.as_ref().map(|(kind, _)| kind.as_str()) != Some(today) {
                wrong.push(format!(
                    "{round} row {number}: {operation} {body}\n  expected a {today}\n  got      {got:?}"
                ));
            }
            continue;
        }
        if got != expected {
            wrong.push(format!(
                "{round} row {number}: {operation} {body}\n  expected {expected:?}\n  got      {got:?}"
            ));
        }
    }
    assert!(compared > 0);
    assert!(
        wrong.is_empty(),
        "{} of {compared} rows differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

#[tokio::test]
async fn round_1_rows_answer_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "round 1", ROUND_1).await;
}

#[tokio::test]
async fn round_2_rows_answer_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "round 2", ROUND_2).await;
}

#[tokio::test]
async fn round_5_rows_answer_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "round 5", ROUND_5).await;
}

#[tokio::test]
async fn round_6_rows_answer_as_dynamodb_did() {
    let url = start_server().await;
    assert_rows(&url, "round 6", ROUND_6).await;
}

/// Every `B` value DynamoDB accepted in a PutItem of rounds 1, 2, 5 and 6 is
/// written, as a non-key attribute, to a table that exists.
#[tokio::test]
async fn accepted_values_are_written_to_a_table_that_exists() {
    let url = start_server().await;
    let mut wrong = Vec::new();
    let mut written = 0;
    for &(_, operation, body, kind, _) in [ROUND_1, ROUND_2, ROUND_5, ROUND_6].concat().iter() {
        let request: serde_json::Value = serde_json::from_str(body).unwrap();
        let value = &request["Item"]["pk"];
        if operation != "PutItem" || kind != "ResourceNotFoundException" || value.get("B").is_none()
        {
            continue;
        }
        written += 1;
        let item = serde_json::json!({
            "TableName": "Tab",
            "Item": {"pk": {"S": format!("k{written}")}, "b": value},
        });
        let got = answer(&url, "PutItem", &item.to_string()).await;
        if got.is_some() {
            wrong.push(format!("{value}: {got:?}"));
        }
    }
    assert!(written > 0);
    assert!(
        wrong.is_empty(),
        "{} of {written} differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// Captured eu-west-2, 2026-11-11: lenient forms written to a table, and the
/// canonical base64 a scan returned for each.
const STORED: &[(&str, &str)] = &[
    ("QQ", "QQ=="),
    ("QQ=", "QQ=="),
    ("QQ==", "QQ=="),
    ("QUI", "QUI="),
    ("QUI=", "QUI="),
    ("abc", "abc="),
    ("abcdefg", "abcdefg="),
    ("QQQQQQ", "QQQQQQ=="),
    ("abcdQUI", "abcdQUI="),
    ("abcdQQ=", "abcdQQ=="),
    ("-Q==", "/Q=="),
    ("{Q==", "/Q=="),
    ("*Q==", "/Q=="),
    ("(Q==", "/Q=="),
    ("~Q==", "/Q=="),
    ("\u{7f}Q==", "/Q=="),
    ("=Q==", "/Q=="),
    ("Q-Q=", "//Q="),
    ("Q=Q=", "//Q="),
    ("-QQ=", "/QQ="),
    ("QQQQ-Q==", "QQQQ/Q=="),
    ("QQQQQ-Q=", "QQQQ//Q="),
    ("--Q=", "//Q="),
    ("-w==", "/w=="),
    ("-/Q=", "//Q="),
    ("/-Q=", "//Q="),
];

#[tokio::test]
async fn accepted_forms_are_stored_as_the_bytes_dynamodb_stored() {
    let url = start_server().await;
    let mut wrong = Vec::new();
    for (i, &(sent, stored)) in STORED.iter().enumerate() {
        let key = format!("k{i:02}");
        let put = serde_json::json!({
            "TableName": "Tab",
            "Item": {"pk": {"S": key}, "b": {"B": sent}},
        });
        let (status, json) = send(&url, "PutItem", &put.to_string()).await;
        if status != 200 {
            wrong.push(format!("{sent:?}: write refused: {json}"));
            continue;
        }
        let get = serde_json::json!({"TableName": "Tab", "Key": {"pk": {"S": key}}});
        let (_, json) = send(&url, "GetItem", &get.to_string()).await;
        let got = &json["Item"]["b"]["B"];
        if got != stored {
            wrong.push(format!("{sent:?}: expected {stored}, read back {got}"));
        }
    }
    // The two writes in the same capture that DynamoDB refused.
    for (sent, message) in [
        ("Q--=", "Invalid last non-pad Base64 character dectected"),
        (" ", "Buffer is too short for base64 encoding"),
    ] {
        let put = serde_json::json!({
            "TableName": "Tab",
            "Item": {"pk": {"S": "refused"}, "b": {"B": sent}},
        });
        let got = answer(&url, "PutItem", &put.to_string()).await;
        let expected = Some((
            "SerializationException".to_string(),
            Some(message.to_string()),
        ));
        if got != expected {
            wrong.push(format!("{sent:?}: expected {expected:?}, got {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Round 3 values, each with the answer DynamoDB gave for it as a `B` on its
/// own (captured eu-west-2, 2026-11-11). Round 1 shows a `BS` member and a
/// `B` nested in an `L` or `M` are read and refused the same way, so the
/// same answers are expected there.
const ROUND_3_SAMPLE: &[(&str, Option<&str>)] = &[
    ("!Q==", Some("")),
    (".QQQ", Some("")),
    ("\u{7f}QQQ", Some("")),
    (
        "Q(==",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        "QQ~=",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    ("(Q==", None),
    ("Q~Q=", None),
];

#[tokio::test]
async fn round_3_values_answer_the_same_in_a_set_and_a_list() {
    let url = start_server().await;
    let mut wrong = Vec::new();
    for &(value, refusal) in ROUND_3_SAMPLE {
        let expected = match refusal {
            // An empty refusal is an exception with no message.
            Some(message) => Some((
                "SerializationException".to_string(),
                Some(message.to_string()).filter(|m| !m.is_empty()),
            )),
            None => Some(("ResourceNotFoundException".to_string(), None)),
        };
        for item in [
            serde_json::json!({"pk": {"BS": [value]}}),
            serde_json::json!({"pk": {"L": [{"B": value}]}}),
            serde_json::json!({"pk": {"M": {"a": {"B": value}}}}),
        ] {
            let body = serde_json::json!({"TableName": "Missing", "Item": item}).to_string();
            let got = answer(&url, "PutItem", &body).await;
            if got != expected {
                wrong.push(format!(
                    "{body}\n  expected {expected:?}\n  got      {got:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Lenient forms written as `BS` members and nested in an `L` and an `M` read
/// back as the canonical base64 DynamoDB stored for each (captured eu-west-2,
/// 2026-11-11, as `B` values; `STORED` above).
#[tokio::test]
async fn accepted_forms_in_sets_lists_and_maps_read_back_canonical() {
    let url = start_server().await;
    let put = serde_json::json!({
        "TableName": "Tab",
        "Item": {
            "pk": {"S": "nested"},
            "bs": {"BS": ["QQ", "QUI", "-QQ=", "QQQQQ-Q="]},
            "l": {"L": [{"B": "Q=Q="}, {"B": "abcdQQ="}]},
            "m": {"M": {"a": {"B": "-w=="}, "b": {"B": "abcdefg"}}},
        },
    });
    let (status, json) = send(&url, "PutItem", &put.to_string()).await;
    assert_eq!(status, 200, "{json}");
    let get = serde_json::json!({"TableName": "Tab", "Key": {"pk": {"S": "nested"}}});
    let (_, json) = send(&url, "GetItem", &get.to_string()).await;
    let item = &json["Item"];
    let mut set: Vec<&str> = item["bs"]["BS"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    set.sort_unstable();
    assert_eq!(set, ["/QQ=", "QQ==", "QQQQ//Q=", "QUI="], "{item}");
    assert_eq!(
        item["l"],
        serde_json::json!({"L": [{"B": "//Q="}, {"B": "abcdQQ=="}]})
    );
    assert_eq!(
        item["m"],
        serde_json::json!({"M": {"a": {"B": "/w=="}, "b": {"B": "abcdefg="}}})
    );
}

/// Round 1: lengths, padding, alphabet, whitespace and non-ASCII; `BS`, nested values; order beside other faults; every operation that takes a value.
const ROUND_1: &[Row] = &[
    (
        1,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":""}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        2,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!"}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        3,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        4,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        5,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!!!"}}}"#,
        "SerializationException",
        None,
    ),
    (
        6,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!!!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        7,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a"}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        8,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        9,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abc"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        10,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        11,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcde"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        12,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdef"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        13,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdefg"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        14,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        15,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abc="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        16,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a==="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        17,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"===="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        18,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"="}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        19,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        20,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab=c"}}}"#,
        "SerializationException",
        None,
    ),
    (
        21,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=abc"}}}"#,
        "SerializationException",
        None,
    ),
    (
        22,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab==abcd"}}}"#,
        "SerializationException",
        None,
    ),
    (
        23,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd===="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        24,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        25,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        26,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QR=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        27,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        28,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QUJ="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        29,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QUI="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        30,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab-_"}}}"#,
        "SerializationException",
        None,
    ),
    (
        31,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a-_="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        32,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab+/"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        33,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"YW Jj"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        34,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"YWJj\n"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        35,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":" YWJj"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        36,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"YWJj "}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        37,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"YW\tJ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        38,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"YW\r\nJj"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        39,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"\u00e9==="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        40,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"YWJ\u00e9"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        41,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab!d"}}}"#,
        "SerializationException",
        None,
    ),
    (
        42,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!bcd"}}}"#,
        "SerializationException",
        None,
    ),
    (
        43,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abc!"}}}"#,
        "SerializationException",
        None,
    ),
    (
        44,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab!="}}}"#,
        "SerializationException",
        None,
    ),
    (
        45,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a!=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        46,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab=!"}}}"#,
        "SerializationException",
        None,
    ),
    (
        47,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab*d!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        48,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        49,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdefg!"}}}"#,
        "SerializationException",
        None,
    ),
    (
        50,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab.d"}}}"#,
        "SerializationException",
        None,
    ),
    (
        51,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab\u0000d"}}}"#,
        "SerializationException",
        None,
    ),
    (
        52,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab~d"}}}"#,
        "SerializationException",
        None,
    ),
    (
        53,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab\u007fd"}}}"#,
        "SerializationException",
        None,
    ),
    (
        54,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab\u0080d"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        55,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab\u00ffd"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        56,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab\u0100d"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        57,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"\ud83d\ude00bcd"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        58,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["!!"]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        59,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["YWJj","!!"]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        60,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["!!!!"]}}}"#,
        "SerializationException",
        None,
    ),
    (
        61,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["!!","!!!"]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        62,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"M":{"a":{"B":"!!"}}}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        63,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"L":[{"B":"!!"}]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        64,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"},"x":{"N":"abc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        65,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"x":{"N":"abc"},"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        66,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"x":{},"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        67,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"x":{"N":1},"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        68,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"},"x":{"N":1}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        69,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","S":"a"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        70,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"S":"a","B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        71,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","NULL":false}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        72,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","BOOL":"x"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        73,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","X":"a"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        74,
        "PutItem",
        r#"{"Item":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        75,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"}},"ReturnValues":"BAD"}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        76,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"}},"ReturnValues":5}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        77,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"},"q":{"B":"!!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        78,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"q":{"B":"!!!"},"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        79,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!"}},"ExpressionAttributeValues":{":v":{"B":"!!!"}},"ConditionExpression":"a = :v"}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        80,
        "PutItem",
        r#"{"TableName":"Missing","ExpressionAttributeValues":{":v":{"B":"!!!"}},"ConditionExpression":"a = :v","Item":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        81,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"S":"a"}},"Expected":{"a":{"Value":{"B":"!!"}}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        82,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"S":"a"}},"Expected":{"a":{"AttributeValueList":[{"B":"!!"}],"ComparisonOperator":"EQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        83,
        "GetItem",
        r#"{"TableName":"Missing","Key":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        84,
        "DeleteItem",
        r#"{"TableName":"Missing","Key":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        85,
        "UpdateItem",
        r#"{"TableName":"Missing","Key":{"pk":{"S":"a"}},"UpdateExpression":"SET a = :v","ExpressionAttributeValues":{":v":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        86,
        "UpdateItem",
        r#"{"TableName":"Missing","Key":{"pk":{"S":"a"}},"AttributeUpdates":{"a":{"Value":{"B":"!!"},"Action":"PUT"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        87,
        "Query",
        r#"{"TableName":"Missing","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        88,
        "Query",
        r#"{"TableName":"Missing","KeyConditions":{"pk":{"AttributeValueList":[{"B":"!!"}],"ComparisonOperator":"EQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        89,
        "Query",
        r#"{"TableName":"Missing","KeyConditionExpression":"pk = :v","ExpressionAttributeValues":{":v":{"S":"a"}},"ExclusiveStartKey":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        90,
        "Scan",
        r#"{"TableName":"Missing","ExclusiveStartKey":{"pk":{"B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        91,
        "Scan",
        r#"{"TableName":"Missing","ScanFilter":{"a":{"AttributeValueList":[{"B":"!!"}],"ComparisonOperator":"EQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        92,
        "BatchWriteItem",
        r#"{"RequestItems":{"Missing":[{"PutRequest":{"Item":{"pk":{"B":"!!"}}}}]}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        93,
        "BatchWriteItem",
        r#"{"RequestItems":{"Missing":[{"DeleteRequest":{"Key":{"pk":{"B":"!!"}}}}]}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        94,
        "BatchGetItem",
        r#"{"RequestItems":{"Missing":{"Keys":[{"pk":{"B":"!!"}}]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        95,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"Missing","Item":{"pk":{"B":"!!"}}}}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        96,
        "TransactWriteItems",
        r#"{"TransactItems":[{"Put":{"TableName":"Missing","Item":{"pk":{"B":"!!"}}}}],"ClientRequestToken":""}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        97,
        "TransactGetItems",
        r#"{"TransactItems":[{"Get":{"TableName":"Missing","Key":{"pk":{"B":"!!"}}}}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        98,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!!!"}]}"#,
        "SerializationException",
        None,
    ),
    (
        99,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"N":"abc"},{"B":"!!"}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        100,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"},{"N":1}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        101,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"N":1},{"B":"!!"}]}"#,
        "SerializationException",
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        102,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"BS":["YWJj","!!"]}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        103,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"M":{"a":{"B":"!!"}}}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        104,
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"}]}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        105,
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"S":"a"}]},{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"}]}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        106,
        "ExecuteTransaction",
        r#"{"TransactStatements":[{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"}]}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        107,
        "ExecuteTransaction",
        r#"{"TransactStatements":[{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"N":"abc"}]},{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"}]}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
];

/// Round 2: unpadded and part-padded values, padding in odd places, invalid characters by position.
const ROUND_2: &[Row] = &[
    (
        1,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        2,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        3,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        4,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        5,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"-Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        6,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        7,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abd"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        8,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        9,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!bc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        10,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a!c"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        11,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a=c"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        12,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=bc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        13,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        14,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"a=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        15,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"==="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        16,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        17,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QUI"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        18,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QUJ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        19,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        20,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"-QQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        21,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ-"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        22,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab "}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        23,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ{"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        24,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":" "}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        25,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"\u00e9"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        26,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"-"}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        27,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        28,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdefghi"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 9"),
    ),
    (
        29,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQ"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        30,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdQ="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        31,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ==QQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        32,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ-Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        33,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdQUI"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        34,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdQUJ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        35,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdQQ="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        36,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd==="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        37,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdef="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        38,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcd!bc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        39,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!bcdabc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        40,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdab!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        41,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"ab-dabc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        42,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcda-c"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        43,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"abcdab-"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        44,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q==="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        45,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ=Q"}}}"#,
        "SerializationException",
        None,
    ),
    (
        46,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=QQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        47,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q=QQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        48,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQ="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        49,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"==QQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        50,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"===Q"}}}"#,
        "SerializationException",
        None,
    ),
    (
        51,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"-Q=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        52,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        53,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-Q="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        54,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"-QQ="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        55,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ-="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        56,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q,=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        57,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q:=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        58,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q@=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        59,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q[=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        60,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q`=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        61,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q{=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        62,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q*=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        63,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q+=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        64,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q/=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        65,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Qw=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        66,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q_=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        67,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ_="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        68,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"{Q=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        69,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"*Q=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        70,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"*QQ="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        71,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q*Q="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        72,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ==QQ=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        73,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ==QQQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        74,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQ=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        75,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ===="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        76,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQ=QQQ="}}}"#,
        "SerializationException",
        None,
    ),
    (
        77,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQ==="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        78,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ=QQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        79,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQ=QQQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        80,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-QQQQ=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        81,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ-Q=="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        82,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"-QQQQQQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        83,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQQ-"}}}"#,
        "SerializationException",
        None,
    ),
    (
        84,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ-QQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        85,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQ-Q="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        86,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQ-=="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        87,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQ==QQQQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        88,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"\u00e9=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        89,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"\u00e9QQ"}}}"#,
        "SerializationException",
        None,
    ),
    (
        90,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ\u00e9"}}}"#,
        "SerializationException",
        None,
    ),
    (
        91,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"\u00e9\u00e9"}}}"#,
        "SerializationException",
        None,
    ),
    (
        92,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q\u00e9="}}}"#,
        "SerializationException",
        None,
    ),
    (
        93,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ\u00e9=="}}}"#,
        "SerializationException",
        None,
    ),
];

/// Round 5: order across faults, order inside one value, `BS` members, PartiQL batches.
const ROUND_5: &[Row] = &[
    (
        1,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-QQQ-=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        2,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!QQQQ-=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        3,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-QQ!Q=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        4,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-QQQQQ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        5,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!QQQQUJ"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        6,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQ="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        7,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQ="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        8,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQQ=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 9"),
    ),
    (
        9,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BOOL":"x","B":"!!"}}}"#,
        "SerializationException",
        Some("Unexpected token received from parser"),
    ),
    (
        10,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","BOOL":"x"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        11,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"S":1,"B":"!!"}}}"#,
        "SerializationException",
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        12,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","S":1}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        13,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","N":"abc"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        14,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"N":"abc","B":"!!"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        15,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","NULL":"x"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        16,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"NULL":"x","B":"!!"}}}"#,
        "SerializationException",
        Some("Unexpected token received from parser"),
    ),
    (
        17,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","B":"QQ=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        18,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ==","B":"!!"}}}"#,
        "SerializationException",
        Some("Duplicate key found in payload"),
    ),
    (
        19,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!","S":null}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        20,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!!!!","S":"a"}}}"#,
        "SerializationException",
        None,
    ),
    (
        21,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q-==","S":"a"}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        22,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!"}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        23,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"!","S":"a"}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        24,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"L":[{"N":1},{"B":"!!"}]}}}"#,
        "SerializationException",
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        25,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"L":[{"B":"!!"},{"N":1}]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        26,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"M":{"a":{"N":1},"b":{"B":"!!"}}}}}"#,
        "SerializationException",
        Some("NUMBER_VALUE cannot be converted to String"),
    ),
    (
        27,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"M":{"b":{"B":"!!"},"a":{"N":1}}}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        28,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["!!",1]}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        29,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":[1,"!!"]}}}"#,
        "SerializationException",
        Some("only base-64-encoded strings are convertible to bytes"),
    ),
    (
        30,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":[]}}}"#,
        "ValidationException",
        Some(
            "1 validation error detected: One or more parameter values were invalid: Binary sets should not be empty",
        ),
    ),
    (
        31,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["QQ","QQ=="]}}}"#,
        "ValidationException",
        Some(
            "1 validation error detected: One or more parameter values were invalid: Input collection [\"QQ==\",\"QQ==\"]of type BS contains duplicates.",
        ),
    ),
    (
        32,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"BS":["QQ==","QQ=="]}}}"#,
        "ValidationException",
        Some(
            "1 validation error detected: One or more parameter values were invalid: Input collection [\"QQ==\",\"QQ==\"]of type BS contains duplicates.",
        ),
    ),
    (
        33,
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!!!"}]}]}"#,
        "SerializationException",
        None,
    ),
    (
        34,
        "BatchExecuteStatement",
        r#"{"Statements":[{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"N":"abc"}]},{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"}]}]}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        35,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"!!"}],"Limit":0}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        36,
        "ExecuteStatement",
        r#"{"Statement":"SELECT * FROM \"Missing\" WHERE pk = ?","Parameters":[{"B":"Q-=="}],"Limit":0}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
];

/// Round 6: values made of padding, and padding after an unpadded length.
const ROUND_6: &[Row] = &[
    (
        1,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=="}}}"#,
        "SerializationException",
        None,
    ),
    (
        2,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"======"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        3,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=========="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 10"),
    ),
    (
        4,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"====="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        5,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"======="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        6,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        7,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQQQ=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 10"),
    ),
    (
        8,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        9,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ==Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        10,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"===Q=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        11,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"=Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        12,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"Q=Q"}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        13,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"===="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        14,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"========"}}}"#,
        "SerializationException",
        None,
    ),
    (
        15,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ===="}}}"#,
        "SerializationException",
        Some("Invalid last non-pad Base64 character dectected"),
    ),
    (
        16,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"="}}}"#,
        "SerializationException",
        Some("Buffer is too short for base64 encoding"),
    ),
    (
        17,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"==="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        18,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQ=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 7"),
    ),
    (
        19,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQQQ==="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 9"),
    ),
    (
        20,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQ===="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 6"),
    ),
    (
        21,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQ=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        22,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"QQQQ="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
    (
        23,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"A="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 2"),
    ),
    (
        24,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"AA"}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        25,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"AA="}}}"#,
        "ResourceNotFoundException",
        None,
    ),
    (
        26,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"A=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 3"),
    ),
    (
        27,
        "PutItem",
        r#"{"TableName":"Missing","Item":{"pk":{"B":"AAA=="}}}"#,
        "SerializationException",
        Some("Base64 encoded length is expected a multiple of 4 bytes but found: 5"),
    ),
];
