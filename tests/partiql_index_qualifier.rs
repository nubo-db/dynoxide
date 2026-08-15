//! `SELECT * FROM "table"."index"`, the PartiQL way of naming an index.
//!
//! Every expectation here was measured against real DynamoDB in eu-west-2 on 15
//! August 2026, against a table keyed `pk`/`sk` carrying one index per
//! projection type on each side. Case labels (Q3, Q12, Q20) refer to that
//! capture.
//!
//! The fixture is built so the awkward cases have somewhere to land. Two items
//! share a `gsiPk`, because a cursor that loses its base table key cannot
//! advance past rows with the same index key and drops them without an error.
//! One item carries no index attribute at all, so every index excludes it and
//! the sparse cases have a negative control.

use dynoxide::Database;
use dynoxide::types::{AttributeValue, ConsumedCapacity};

const TABLE: &str = "pq_idxq";

/// The string behind an `S` attribute, or `None` for anything else.
fn string_of(v: &AttributeValue) -> Option<String> {
    match v {
        AttributeValue::S(s) => Some(s.clone()),
        _ => None,
    }
}

fn table_def() -> serde_json::Value {
    serde_json::json!({
        "TableName": TABLE,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsiPk", "AttributeType": "S"},
            {"AttributeName": "gsiPk2", "AttributeType": "S"},
            {"AttributeName": "lsiSk", "AttributeType": "S"},
            {"AttributeName": "lsiSk2", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "gsi-all",
                "KeySchema": [{"AttributeName": "gsiPk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "gsi-inc",
                "KeySchema": [{"AttributeName": "gsiPk2", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["projattr"]}
            },
            {
                "IndexName": "gsi-keys",
                "KeySchema": [{"AttributeName": "gsiPk2", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }
        ],
        "LocalSecondaryIndexes": [
            {
                "IndexName": "lsi-all",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsiSk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "lsi-keys",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsiSk2", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }
        ]
    })
}

fn seeded() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(serde_json::from_value(table_def()).unwrap())
        .unwrap();
    for item in [
        serde_json::json!({
            "pk": {"S": "p"}, "sk": {"S": "s1"},
            "gsiPk": {"S": "x"}, "gsiPk2": {"S": "x2"},
            "lsiSk": {"S": "l1"}, "lsiSk2": {"S": "m1"},
            "projattr": {"S": "P1"}, "nonproj": {"S": "N1"}
        }),
        serde_json::json!({
            "pk": {"S": "p"}, "sk": {"S": "s2"},
            "gsiPk": {"S": "y"}, "gsiPk2": {"S": "y2"},
            "lsiSk": {"S": "l2"}, "lsiSk2": {"S": "m2"},
            "projattr": {"S": "P2"}, "nonproj": {"S": "N2"}
        }),
        // No index attribute at all, so every index excludes it.
        serde_json::json!({
            "pk": {"S": "p"}, "sk": {"S": "s3"},
            "projattr": {"S": "P3"}, "nonproj": {"S": "N3"}
        }),
        // Shares gsiPk=x with s1, for the tied-key pagination cases.
        serde_json::json!({
            "pk": {"S": "p"}, "sk": {"S": "s4"},
            "gsiPk": {"S": "x"}, "gsiPk2": {"S": "x2"},
            "lsiSk": {"S": "l4"}, "lsiSk2": {"S": "m4"},
            "projattr": {"S": "P4"}, "nonproj": {"S": "N4"}
        }),
    ] {
        db.put_item(
            serde_json::from_value(serde_json::json!({"TableName": TABLE, "Item": item})).unwrap(),
        )
        .unwrap();
    }
    db
}

/// The sorted `pk/sk` of every row a statement returned.
fn keys(db: &Database, sql: &str) -> Vec<String> {
    let req = serde_json::json!({"Statement": sql});
    let resp = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap();
    let mut out: Vec<String> = resp
        .items
        .unwrap_or_default()
        .iter()
        .map(|i| {
            format!(
                "{}/{}",
                i.get("pk").and_then(string_of).unwrap_or_default(),
                i.get("sk").and_then(string_of).unwrap_or_default()
            )
        })
        .collect();
    out.sort();
    out
}

/// The sorted attribute names a statement's rows carried.
fn attributes(db: &Database, sql: &str) -> Vec<String> {
    let req = serde_json::json!({"Statement": sql});
    let resp = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap();
    let mut names: Vec<String> = resp
        .items
        .unwrap_or_default()
        .iter()
        .flat_map(|i| i.keys().cloned())
        .collect();
    names.sort();
    names.dedup();
    names
}

fn capacity(db: &Database, sql: &str) -> ConsumedCapacity {
    let req = serde_json::json!({"Statement": sql, "ReturnConsumedCapacity": "INDEXES"});
    db.execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity")
}

fn error(db: &Database, sql: &str) -> String {
    let req = serde_json::json!({"Statement": sql});
    db.execute_statement(serde_json::from_value(req).unwrap())
        .expect_err("expected a rejection")
        .to_string()
}

// --- membership and the WHERE clause ------------------------------------

#[test]
fn an_index_select_returns_only_index_members() {
    // Q2. `s3` carries no gsiPk, so a sparse GSI excludes it. Before the
    // qualifier was parsed this scanned the base table and handed it back.
    let db = seeded();
    assert_eq!(
        keys(&db, &format!("SELECT * FROM \"{TABLE}\".\"gsi-all\"")),
        vec!["p/s1", "p/s2", "p/s4"]
    );
}

#[test]
fn an_index_select_keeps_its_where_clause() {
    // Q3. The qualifier used to strand the WHERE clause, so this returned the
    // whole table.
    let db = seeded();
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE gsiPk='x'")
        ),
        vec!["p/s1", "p/s4"]
    );
}

#[test]
fn an_unqualified_select_is_unchanged() {
    // Q1. The control: filtering on an index key attribute without naming the
    // index still reads the base table.
    let db = seeded();
    assert_eq!(
        keys(&db, &format!("SELECT * FROM \"{TABLE}\" WHERE gsiPk='x'")),
        vec!["p/s1", "p/s4"]
    );
}

#[test]
fn a_filter_on_a_projected_non_key_attribute_applies() {
    // Q4. `nonproj` is not an index key but an ALL projection carries it.
    let db = seeded();
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE nonproj='N1'")
        ),
        vec!["p/s1"]
    );
}

#[test]
fn a_gsi_filter_on_an_unprojected_attribute_matches_nothing() {
    // Q7. The attribute is absent from the index entry, so nothing matches and
    // the read does not reach back to the base table to find out.
    let db = seeded();
    assert!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE nonproj='N1'")
        )
        .is_empty()
    );
}

// --- projection ----------------------------------------------------------

#[test]
fn an_include_index_returns_its_projected_attributes() {
    // Q5. Index key, table keys, and the named non-key attribute.
    let db = seeded();
    assert_eq!(
        attributes(&db, &format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\"")),
        vec!["gsiPk2", "pk", "projattr", "sk"]
    );
}

#[test]
fn a_keys_only_index_returns_keys_alone() {
    // Q6.
    let db = seeded();
    assert_eq!(
        attributes(&db, &format!("SELECT * FROM \"{TABLE}\".\"gsi-keys\"")),
        vec!["gsiPk2", "pk", "sk"]
    );
}

#[test]
fn a_keys_only_lsi_returns_keys_alone() {
    // Q10.
    let db = seeded();
    assert_eq!(
        attributes(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p'")
        ),
        vec!["lsiSk2", "pk", "sk"]
    );
}

// --- LSI -----------------------------------------------------------------

#[test]
fn an_lsi_select_reads_the_index() {
    // Q9.
    let db = seeded();
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"lsi-all\" WHERE pk='p'")
        ),
        vec!["p/s1", "p/s2", "p/s4"]
    );
}

#[test]
fn an_lsi_select_without_a_partition_key_still_reads_the_index() {
    // Q11. An unkeyed read of an LSI is a scan of it, not a rejection.
    let db = seeded();
    assert_eq!(
        keys(&db, &format!("SELECT * FROM \"{TABLE}\".\"lsi-all\"")),
        vec!["p/s1", "p/s2", "p/s4"]
    );
}

// --- rejections ----------------------------------------------------------

#[test]
fn an_unknown_index_is_rejected_without_naming_it() {
    // Q12. Query and Scan append the index name through the helpers in
    // actions::gsi and actions::lsi. AWS does not append it here, so this
    // message is deliberately not the one those helpers build.
    let db = seeded();
    let msg = error(&db, &format!("SELECT * FROM \"{TABLE}\".\"nosuchindex\""));
    assert!(
        msg.contains("The table does not have the specified index"),
        "got {msg}"
    );
    assert!(
        !msg.contains("nosuchindex"),
        "the message must not name the index: {msg}"
    );
}

#[test]
fn a_qualifier_naming_the_table_is_rejected() {
    // Q13.
    let db = seeded();
    let msg = error(&db, &format!("SELECT * FROM \"{TABLE}\".\"{TABLE}\""));
    assert!(
        msg.contains("The table does not have the specified index"),
        "got {msg}"
    );
}

#[test]
fn a_consistent_read_against_a_gsi_is_rejected() {
    // Q16. PartiQL words this differently from Query, which says "Consistent
    // reads are not supported on global secondary indexes". Both wordings were
    // captured on the same day, so neither is a stale copy of the other.
    let db = seeded();
    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE gsiPk='x'"),
        "ConsistentRead": true
    });
    let msg = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .expect_err("a consistent GSI read is rejected")
        .to_string();
    assert!(
        msg.contains("Strongly consistent read is not supported on Global Secondary Indexes"),
        "got {msg}"
    );
}

#[test]
fn a_consistent_read_against_an_lsi_is_allowed() {
    // Q17. An LSI is on the same partition as the table, so it can be read
    // consistently.
    let db = seeded();
    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\".\"lsi-all\" WHERE pk='p'"),
        "ConsistentRead": true,
        "ReturnConsumedCapacity": "INDEXES"
    });
    let resp = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .expect("a consistent LSI read is allowed");
    assert_eq!(resp.items.unwrap_or_default().len(), 3);
    let cap = resp.consumed_capacity.expect("INDEXES reports capacity");
    // Consistency doubles the index arm the way it doubles a table arm.
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(0.0));
    assert_eq!(
        cap.local_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("lsi-all"))
            .map(|d| d.capacity_units),
        Some(1.0)
    );
}

#[test]
fn a_qualifier_on_a_write_statement_is_rejected() {
    // Q23, Q24. DynamoDB rejects these before it resolves the table, so a
    // qualified UPDATE against a table that does not exist still reports the
    // index rather than the missing table.
    let db = seeded();
    for sql in [
        format!("UPDATE \"{TABLE}\".\"gsi-all\" SET projattr='z' WHERE pk='p' AND sk='s1'"),
        format!("DELETE FROM \"{TABLE}\".\"gsi-all\" WHERE pk='p' AND sk='s1'"),
        "UPDATE \"nosuchtable\".\"gsi-all\" SET a='z' WHERE pk='p'".to_string(),
    ] {
        let msg = error(&db, &sql);
        assert!(
            msg.contains("This operation is not supported on an index"),
            "for {sql}: got {msg}"
        );
    }
}

// --- capacity ------------------------------------------------------------

#[test]
fn a_keyed_gsi_select_is_charged_to_the_index_arm() {
    // Q3: total 0.5, table 0, gsi-all 0.5. Before this, dynoxide charged the
    // base table because a base table scan is what ran.
    let db = seeded();
    let cap = capacity(
        &db,
        &format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE gsiPk='x'"),
    );
    assert_eq!(cap.capacity_units, 0.5);
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(0.0));
    assert_eq!(
        cap.global_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("gsi-all"))
            .map(|d| d.capacity_units),
        Some(0.5)
    );
    assert!(cap.local_secondary_indexes.is_none());
}

#[test]
fn a_keyed_lsi_select_is_charged_to_the_index_arm() {
    // Q9: total 0.5, table 0, lsi-all 0.5.
    let db = seeded();
    let cap = capacity(
        &db,
        &format!("SELECT * FROM \"{TABLE}\".\"lsi-all\" WHERE pk='p'"),
    );
    assert_eq!(cap.capacity_units, 0.5);
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(0.0));
    assert_eq!(
        cap.local_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("lsi-all"))
            .map(|d| d.capacity_units),
        Some(0.5)
    );
}

#[test]
fn an_unqualified_select_is_still_charged_to_the_table() {
    let db = seeded();
    let cap = capacity(
        &db,
        &format!("SELECT * FROM \"{TABLE}\" WHERE pk='p' AND sk='s1'"),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(0.5));
    assert!(cap.global_secondary_indexes.is_none());
}

#[test]
fn total_mode_reports_the_same_total_without_arms() {
    let db = seeded();
    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE gsiPk='x'"),
        "ReturnConsumedCapacity": "TOTAL"
    });
    let cap = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("TOTAL reports capacity");
    assert_eq!(cap.capacity_units, 0.5);
    assert!(cap.global_secondary_indexes.is_none());
    assert!(cap.table.is_none());
}

#[test]
fn none_mode_reports_no_capacity() {
    let db = seeded();
    let req = serde_json::json!({
        "Statement": format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE gsiPk='x'")
    });
    let resp = db
        .execute_statement(serde_json::from_value(req).unwrap())
        .unwrap();
    assert!(resp.consumed_capacity.is_none());
}

// --- pagination ----------------------------------------------------------

#[test]
fn a_continuation_across_tied_index_keys_advances() {
    // Q19, Q20. `s1` and `s4` share gsiPk=x. A cursor carrying only the index
    // key cannot tell them apart, so the second page repeats the first row or
    // skips it. The base table key in the token is what breaks the tie.
    let db = seeded();
    let sql = format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE gsiPk='x'");

    let first = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({"Statement": sql, "Limit": 1})).unwrap(),
        )
        .unwrap();
    let first_keys: Vec<String> = first
        .items
        .clone()
        .unwrap_or_default()
        .iter()
        .map(|i| i.get("sk").and_then(string_of).unwrap_or_default())
        .collect();
    let token = first
        .next_token
        .expect("a limited read owes a continuation");

    let second = db
        .execute_statement(
            serde_json::from_value(
                serde_json::json!({"Statement": sql, "Limit": 1, "NextToken": token}),
            )
            .unwrap(),
        )
        .unwrap();
    let second_keys: Vec<String> = second
        .items
        .unwrap_or_default()
        .iter()
        .map(|i| i.get("sk").and_then(string_of).unwrap_or_default())
        .collect();

    assert_eq!(first_keys, vec!["s1"]);
    assert_eq!(
        second_keys,
        vec!["s4"],
        "the tied key repeated or was skipped"
    );
}

#[test]
fn a_token_minted_against_one_index_is_rejected_by_another() {
    // Q21. The index is part of the row walk, not just the filter, so a token
    // replayed against a different index would resume at a position that means
    // nothing there.
    let db = seeded();
    let first = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": format!("SELECT * FROM \"{TABLE}\".\"gsi-all\""),
                "Limit": 1
            }))
            .unwrap(),
        )
        .unwrap();
    let token = first
        .next_token
        .expect("a limited read owes a continuation");

    let msg = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\""),
                "Limit": 1,
                "NextToken": token
            }))
            .unwrap(),
        )
        .expect_err("a cross-index token is rejected")
        .to_string();
    assert!(
        msg.contains("NextToken does not match request"),
        "got {msg}"
    );
}

#[test]
fn a_token_minted_against_the_table_is_rejected_by_an_index() {
    let db = seeded();
    let first = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": format!("SELECT * FROM \"{TABLE}\""),
                "Limit": 1
            }))
            .unwrap(),
        )
        .unwrap();
    let token = first
        .next_token
        .expect("a limited read owes a continuation");

    let msg = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": format!("SELECT * FROM \"{TABLE}\".\"gsi-all\""),
                "Limit": 1,
                "NextToken": token
            }))
            .unwrap(),
        )
        .expect_err("a base table token is rejected by an index read")
        .to_string();
    assert!(
        msg.contains("NextToken does not match request"),
        "got {msg}"
    );
}
