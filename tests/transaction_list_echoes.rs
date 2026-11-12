//! A statement or get list longer than a transaction allows, and a
//! BatchWriteItem request, is echoed in its constraint message as the Java
//! model's `toString`: every field of every entry named, every attribute value
//! with all sixteen of its fields, and a map's entries in the order of a Java
//! hash map. An empty list is echoed as `[]`. Each expected head and tail below
//! is DynamoDB's own text for the same request (captured us-east-1, eu-west-1
//! and us-west-2, 2026-10-10; eu-west-2 echoes no list for TransactGetItems,
//! and some hosts print a value as an object hash, so the form most regions
//! give is the one followed). The requests use the capture's own table name,
//! so the whole message can be compared by length too. TransactWriteItems
//! echoes no list (captured eu-west-2 and us-east-1, 2026-11-11).

use dynoxide::Database;
use dynoxide::actions::execute_transaction::ExecuteTransactionRequest;
use dynoxide::actions::transact_get_items::TransactGetItemsRequest;
use dynoxide::actions::transact_write_items::TransactWriteItemsRequest;
use serde_json::{Value, json};

const TABLE: &str = "_capture_20261010_mcii_none";

fn assert_echo(message: &str, head: &str, tail: &str, len: usize) {
    assert!(
        message.starts_with(head),
        "head differs:\n{}\n---\n{}",
        &message[..message.len().min(head.len() + 200)],
        head
    );
    assert!(
        message.ends_with(tail),
        "tail differs: {}",
        &message[message.len().saturating_sub(400)..]
    );
    assert_eq!(message.len(), len);
}

#[test]
fn an_empty_statement_list_is_echoed_as_brackets() {
    let db = Database::memory().unwrap();
    let err = db
        .execute_transaction(
            serde_json::from_value::<ExecuteTransactionRequest>(json!({"TransactStatements": []}))
                .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: Value '[]' at 'transactStatements' failed to satisfy \
         constraint: Member must have length greater than or equal to 1"
    );
}

/// The capture's own request: its first statement carries
/// `ReturnValuesOnConditionCheckFailure: ALL_OLD`, echoed as sent, and the
/// rest leave it out and are echoed `null`.
#[test]
fn a_statement_list_over_100_is_echoed_in_the_java_form() {
    let db = Database::memory().unwrap();
    let mut statements = vec![json!({
        "Statement": format!("UPDATE \"{TABLE}\" SET a = ? WHERE pk = ?"),
        "Parameters": [{"N": "1"}, {"S": "k"}],
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
    })];
    statements.extend(
        (0..100).map(|_| json!({"Statement": format!("SELECT * FROM \"{TABLE}\" WHERE pk = 'a'")})),
    );
    let err = db
        .execute_transaction(
            serde_json::from_value::<ExecuteTransactionRequest>(
                json!({"TransactStatements": statements}),
            )
            .unwrap(),
        )
        .unwrap_err();
    assert_echo(
        &err.to_string(),
        concat!(
            "1 validation error detected: Value '[ParameterizedStatement(statement=UPDATE \"_capture_20261010_mcii_none\" SET a = ? WHERE pk = ?",
            ", parameters=[AttributeValue(s=null, n=1, b=null, iNT=null, dECIMAL=null",
            ", dOUBLE=null, sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null",
            ", dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null), AttributeValue(s=k",
            ", n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null",
            ", bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null",
            ", nULL=null, bOOL=null)], returnValuesOnConditionCheckFailure=ALL_OLD)",
            ", ParameterizedStatement(statement=SELECT * FROM \"_capture_20261010_mcii_none\" WHERE pk = 'a'",
            ", parameters=null, returnValuesOnConditionCheckFailure=null), ",
        ),
        concat!(
            "returnValuesOnConditionCheckFailure=null)",
            ", ParameterizedStatement(statement=SELECT * FROM \"_capture_20261010_mcii_none\" WHERE pk = 'a'",
            ", parameters=null",
            ", returnValuesOnConditionCheckFailure=null)]' at 'transactStatements' failed to satisfy constraint: Member must have length less than or equal to 100",
        ),
        15964,
    );
}

/// TransactWriteItems is the exception: it refuses more than 100 actions in
/// the older form, with no list echoed (captured eu-west-2 and us-east-1,
/// 2026-11-11).
#[test]
fn an_action_list_over_100_is_not_echoed() {
    let db = Database::memory().unwrap();
    let key = json!({"pk": {"S": "k"}});
    let full = |mut action: Value| {
        action["TableName"] = json!(TABLE);
        action["ConditionExpression"] = json!("#n = :v");
        action["ExpressionAttributeNames"] = json!({"#n": "a"});
        action["ExpressionAttributeValues"] = json!({":v": {"N": "1"}});
        action["ReturnValuesOnConditionCheckFailure"] = json!("ALL_OLD");
        action
    };
    let mut items = vec![
        json!({"ConditionCheck": full(json!({"Key": key}))}),
        json!({"Delete": full(json!({"Key": key}))}),
        json!({"Update": full(json!({"Key": key, "UpdateExpression": "SET #n = :v"}))}),
        json!({"Put": full(json!({"Item": {
            "pk": {"S": "p"},
            "b": {"BOOL": true},
            "l": {"L": [{"N": "1"}]},
            "m": {"M": {"x": {"S": "y"}}},
            "ss": {"SS": ["a"]},
            "nul": {"NULL": true},
            "bin": {"B": "YWJj"}
        }}))}),
    ];
    items.extend(
        (0..97)
            .map(|i| json!({"Put": {"TableName": TABLE, "Item": {"pk": {"S": format!("t{i}")}}}})),
    );
    let err = db
        .transact_write_items(
            serde_json::from_value::<TransactWriteItemsRequest>(json!({"TransactItems": items}))
                .unwrap(),
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "1 validation error detected: Value at 'TransactItems' failed to satisfy constraint: \
         Member must have length less than or equal to 100"
    );
}

#[test]
fn a_get_list_over_100_is_echoed_in_the_java_form() {
    let db = Database::memory().unwrap();
    let mut items = vec![json!({"Get": {
        "TableName": TABLE,
        "Key": {"pk": {"S": "k"}},
        "ProjectionExpression": "#n",
        "ExpressionAttributeNames": {"#n": "a"}
    }})];
    items.extend(
        (0..100)
            .map(|i| json!({"Get": {"TableName": TABLE, "Key": {"pk": {"S": format!("g{i}")}}}})),
    );
    let err = db
        .transact_get_items(
            serde_json::from_value::<TransactGetItemsRequest>(json!({"TransactItems": items}))
                .unwrap(),
        )
        .unwrap_err();
    assert_echo(
        &err.to_string(),
        concat!(
            "1 validation error detected: Value '[TransactGetItem(get=Get(key={pk=AttributeValue(s=k",
            ", n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null",
            ", bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null",
            ", nULL=null, bOOL=null)}, tableName=_capture_20261010_mcii_none",
            ", projectionExpression=#n, expressionAttributeNames={#n=a}",
            ", workloadProfileName=null))",
            ", TransactGetItem(get=Get(key={pk=AttributeValue(s=g0, n=null, b=null",
            ", iNT=null, dECIMAL=null, dOUBLE=null, sS=null, nS=null, bS=null",
            ", iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null",
            ", bOOL=null)}, tableName=_capture_20261010_mcii_none",
            ", projectionExpression=null, expressionAttributeNames=null",
            ", workloadProfileName=null)), ",
        ),
        concat!(
            "l, dECIMALSET=null, dOUBLESET=null, m=null, l=null, nULL=null, bOOL=null)}",
            ", tableName=_capture_20261010_mcii_none, projectionExpression=null",
            ", expressionAttributeNames=null",
            ", workloadProfileName=null))]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100",
        ),
        34969,
    );
}

/// The names of the map that opens at `start` in `message` (just after its
/// `{`), in the order the echo lists them.
fn echoed_names(message: &str, start: usize) -> Vec<String> {
    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut name = String::new();
    for c in message[start..].chars() {
        match c {
            '{' | '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            '}' if depth == 0 => break,
            '}' => depth -= 1,
            '=' if depth == 0 && !name.is_empty() => names.push(std::mem::take(&mut name)),
            ',' if depth == 0 => name.clear(),
            ' ' if depth == 0 => {}
            _ if depth == 0 => name.push(c),
            _ => {}
        }
    }
    names
}

/// An echoed map is a Java `HashMap` sized to its entries (`new HashMap<>(n)`),
/// so its names come out in the buckets of a table of 2, 4, 8, 16, 32, 64 or
/// 128 as the count grows, never a default table of sixteen. Each `sent` list
/// is the capture's request, in the order it was sent, and each `listed` list
/// is DynamoDB's echo of it (us-east-1 and eu-west-2, 2026-10-10), except
/// that names sharing a bucket are listed here in name order: DynamoDB keeps
/// them in the order they were sent, which a decoded request no longer holds.
#[test]
fn an_echoed_map_is_sized_to_its_entries() {
    let cases: [(&[&str], &[&str]); 11] = [
        (&["bkem", "pk"], &["bkem", "pk"]),
        (&["a001", "a000", "pk"], &["a001", "a000", "pk"]),
        (
            &["bcsb", "bdrw", "bugl", "pk"],
            &["bdrw", "pk", "bcsb", "bugl"],
        ),
        (
            &["bsol", "byko", "bvrn", "btgp", "pk"],
            &["bsol", "pk", "btgp", "byko", "bvrn"],
        ),
        (
            &["bkxo", "bjqp", "bhcs", "bfwy", "bjhz", "pk"],
            &["bfwy", "bhcs", "bjqp", "pk", "bjhz", "bkxo"],
        ),
        (
            &[
                "a010", "a009", "a008", "a007", "a006", "a005", "a004", "a003", "a002", "a001",
                "a000", "pk",
            ],
            &[
                "a010", "a000", "a009", "a005", "a006", "a007", "a008", "pk", "a001", "a002",
                "a003", "a004",
            ],
        ),
        (
            &[
                "a011", "a010", "a009", "a008", "a007", "a006", "a005", "a004", "a003", "a002",
                "a001", "a000", "pk",
            ],
            &[
                "a010", "a000", "a011", "a009", "a005", "a006", "a007", "a008", "pk", "a001",
                "a002", "a003", "a004",
            ],
        ),
        (
            &[
                "a022", "a021", "a020", "a019", "a018", "a017", "a016", "a015", "a014", "a013",
                "a012", "a011", "a010", "a009", "a008", "a007", "a006", "a005", "a004", "a003",
                "a002", "a001", "a000", "pk",
            ],
            &[
                "a020", "a010", "a021", "a000", "a011", "a022", "a009", "a005", "a016", "a006",
                "a017", "a007", "a018", "a008", "a019", "pk", "a001", "a012", "a002", "a013",
                "a003", "a014", "a004", "a015",
            ],
        ),
        (
            &[
                "a023", "a022", "a021", "a020", "a019", "a018", "a017", "a016", "a015", "a014",
                "a013", "a012", "a011", "a010", "a009", "a008", "a007", "a006", "a005", "a004",
                "a003", "a002", "a001", "a000", "pk",
            ],
            &[
                "a020", "a021", "a000", "a022", "a009", "a005", "a006", "a007", "a008", "a001",
                "a023", "a002", "a003", "a004", "a010", "a011", "a016", "a017", "a018", "a019",
                "pk", "a012", "a013", "a014", "a015",
            ],
        ),
        (
            &[
                "a046", "a045", "a044", "a043", "a042", "a041", "a040", "a039", "a038", "a037",
                "a036", "a035", "a034", "a033", "a032", "a031", "a030", "a029", "a028", "a027",
                "a026", "a025", "a024", "a023", "a022", "a021", "a020", "a019", "a018", "a017",
                "a016", "a015", "a014", "a013", "a012", "a011", "a010", "a009", "a008", "a007",
                "a006", "a005", "a004", "a003", "a002", "a001", "a000", "pk",
            ],
            &[
                "a041", "a020", "a042", "a021", "a043", "a000", "a022", "a044", "a040", "a009",
                "a005", "a027", "a006", "a028", "a007", "a029", "a008", "a001", "a023", "a045",
                "a002", "a024", "a046", "a003", "a025", "a004", "a026", "a030", "a031", "a010",
                "a032", "a011", "a033", "a016", "a038", "a017", "a039", "a018", "a019", "pk",
                "a012", "a034", "a013", "a035", "a014", "a036", "a015", "a037",
            ],
        ),
        (
            &[
                "a047", "a046", "a045", "a044", "a043", "a042", "a041", "a040", "a039", "a038",
                "a037", "a036", "a035", "a034", "a033", "a032", "a031", "a030", "a029", "a028",
                "a027", "a026", "a025", "a024", "a023", "a022", "a021", "a020", "a019", "a018",
                "a017", "a016", "a015", "a014", "a013", "a012", "a011", "a010", "a009", "a008",
                "a007", "a006", "a005", "a004", "a003", "a002", "a001", "a000", "pk",
            ],
            &[
                "a041", "a042", "a043", "a000", "a044", "a040", "a009", "a005", "a006", "a007",
                "a008", "a001", "a045", "a002", "a046", "a003", "a047", "a004", "a030", "a031",
                "a032", "a033", "a038", "a039", "a034", "a035", "a036", "a037", "a020", "a021",
                "a022", "a027", "a028", "a029", "a023", "a024", "a025", "a026", "a010", "a011",
                "a016", "a017", "a018", "a019", "pk", "a012", "a013", "a014", "a015",
            ],
        ),
    ];
    let db = Database::memory().unwrap();
    for (sent, listed) in cases {
        let item: serde_json::Map<String, Value> = sent
            .iter()
            .map(|name| {
                let value = if *name == "pk" {
                    json!({"S": "k"})
                } else {
                    json!({"N": "1"})
                };
                (name.to_string(), value)
            })
            .collect();

        let mut puts = vec![json!({"PutRequest": {"Item": item}})];
        puts.extend(
            (0..25).map(|i| json!({"PutRequest": {"Item": {"pk": {"S": format!("s{i}")}}}})),
        );
        let message = db
            .batch_write_item(serde_json::from_value(json!({"RequestItems": {"t": puts}})).unwrap())
            .unwrap_err()
            .to_string();
        let start = message.find("item={").unwrap() + "item={".len();
        assert_eq!(
            echoed_names(&message, start),
            listed,
            "batch, {} names",
            sent.len()
        );
    }

    // A nested map is sized the same way.
    let sent = ["bsol", "byko", "bvrn", "btgp", "pk"];
    let nested: serde_json::Map<String, Value> = sent
        .iter()
        .map(|name| (name.to_string(), json!({"N": "1"})))
        .collect();
    let mut puts = vec![json!({"PutRequest": {"Item": {
        "pk": {"S": "nest"},
        "m": {"M": nested}
    }}})];
    puts.extend((0..25).map(|i| json!({"PutRequest": {"Item": {"pk": {"S": format!("s{i}")}}}})));
    let message = db
        .batch_write_item(serde_json::from_value(json!({"RequestItems": {"t": puts}})).unwrap())
        .unwrap_err()
        .to_string();
    let start = message.find(", m={").unwrap() + ", m={".len();
    assert_eq!(
        echoed_names(&message, start),
        ["bsol", "pk", "btgp", "byko", "bvrn"]
    );

    // So is ExpressionAttributeNames, echoed here in a get list.
    let names: serde_json::Map<String, Value> = sent
        .iter()
        .map(|name| (format!("#{name}"), json!(name)))
        .collect();
    let projection = sent
        .iter()
        .map(|name| format!("#{name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut gets = vec![json!({"Get": {
        "TableName": "t",
        "Key": {"pk": {"S": "n"}},
        "ProjectionExpression": projection,
        "ExpressionAttributeNames": names
    }})];
    gets.extend(
        (0..100).map(|i| json!({"Get": {"TableName": "t", "Key": {"pk": {"S": format!("g{i}")}}}})),
    );
    let message = db
        .transact_get_items(
            serde_json::from_value::<TransactGetItemsRequest>(json!({"TransactItems": gets}))
                .unwrap(),
        )
        .unwrap_err()
        .to_string();
    let start =
        message.find("expressionAttributeNames={").unwrap() + "expressionAttributeNames={".len();
    assert_eq!(
        echoed_names(&message, start),
        ["#bvrn", "#bsol", "#btgp", "#byko", "#pk"]
    );
}
