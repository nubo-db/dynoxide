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

/// The capacity a whole request reports, for the cases that need to set more
/// than the statement.
fn capacity_in(db: &Database, req: serde_json::Value) -> ConsumedCapacity {
    db.execute_statement(serde_json::from_value(req).unwrap())
        .unwrap()
        .consumed_capacity
        .expect("INDEXES mode reports capacity")
}

/// The capacity a statement reports under `INDEXES`.
fn capacity(db: &Database, sql: &str) -> ConsumedCapacity {
    capacity_in(
        db,
        serde_json::json!({"Statement": sql, "ReturnConsumedCapacity": "INDEXES"}),
    )
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

#[test]
fn a_gsi_rejects_a_projection_it_does_not_carry() {
    // Q8, Q30. A GSI cannot reach back to the base table, so naming an
    // attribute it does not carry is rejected. An LSI accepts the same
    // statement and serves it from the table.
    let db = seeded();
    let msg = error(&db, &format!("SELECT nonproj FROM \"{TABLE}\".\"gsi-inc\""));
    assert!(
        msg.contains(
            "One or more parameter values were invalid: \
             Global secondary index gsi-inc does not project [nonproj]"
        ),
        "got {msg}"
    );
}

#[test]
fn a_gsi_accepts_a_projection_it_does_carry() {
    let db = seeded();
    assert_eq!(
        attributes(
            &db,
            &format!("SELECT pk, projattr FROM \"{TABLE}\".\"gsi-inc\"")
        ),
        vec!["pk", "projattr"]
    );
}

#[test]
fn a_keyed_read_rejects_a_filter_on_an_attribute_the_index_does_not_carry() {
    // Q34, Q28. Both kinds reject it, and the message says "Secondary index"
    // with neither Global nor Local in front.
    let db = seeded();
    for (sql, index) in [
        (
            format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND nonproj='N1'"),
            "gsi-inc",
        ),
        (
            format!("SELECT * FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p' AND projattr='P1'"),
            "lsi-keys",
        ),
    ] {
        let msg = error(&db, &sql);
        assert!(
            msg.contains(&format!(
                "One or more parameter values were invalid: Secondary index {index} \
                 does not project one or more filter attributes:"
            )),
            "for {sql}: got {msg}"
        );
    }
}

#[test]
fn an_unkeyed_read_accepts_a_filter_the_index_cannot_satisfy() {
    // Q7, Q29, Q35, Q37. Without a condition on the index partition key the
    // read is a scan, and a scan matches nothing rather than failing. These
    // four are why the rule above is not a GSI-versus-LSI split: Q37 is the LSI
    // mirror of Q7, and Q35 rules out the condition count as the trigger.
    let db = seeded();
    for sql in [
        format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE nonproj='N1'"),
        format!("SELECT * FROM \"{TABLE}\".\"gsi-keys\" WHERE projattr='P1'"),
        format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE nonproj='N1' AND projattr='P1'"),
        format!("SELECT * FROM \"{TABLE}\".\"lsi-keys\" WHERE projattr='P1'"),
    ] {
        assert!(keys(&db, &sql).is_empty(), "{sql} should match nothing");
    }
}

#[test]
fn an_in_on_the_index_key_counts_as_keyed() {
    // R12. An IN cannot be pushed down as a single key so the read still scans,
    // but AWS rejects an unprojected filter alongside it all the same. The
    // rejection follows the shape of the key condition, not what the read does
    // with it.
    let db = seeded();
    let msg = error(
        &db,
        &format!(
            "SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2 IN ['x2','y2'] AND nonproj='N1'"
        ),
    );
    assert!(
        msg.contains("does not project one or more filter attributes"),
        "got {msg}"
    );
}

#[test]
fn an_index_key_reached_through_or_does_not_count_as_keyed() {
    // R11. AWS accepts an unprojected filter when the index key is reached
    // through OR, so the rejection must not fire on a multi-group WHERE.
    //
    // The captured statement parenthesised its groups; this one cannot, because
    // dynoxide's PartiQL parser rejects parentheses in a WHERE clause outright.
    // That gap is not this change's to fix, so the unparenthesised form stands
    // in, and what is pinned here is the narrower claim: a multi-group WHERE is
    // not treated as keyed.
    let db = seeded();
    let sql = format!(
        "SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND nonproj='N1' OR gsiPk2='y2'"
    );
    let req = serde_json::json!({"Statement": sql});
    assert!(
        db.execute_statement(serde_json::from_value(req).unwrap())
            .is_ok(),
        "a multi-group WHERE must not trigger the unprojected-filter rejection"
    );
}

#[test]
fn a_non_equality_predicate_on_an_unprojected_attribute_is_rejected_when_keyed() {
    // R4, R5, R6. The rejection covers every predicate that reads the
    // attribute, not just equality.
    let db = seeded();
    for sql in [
        format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND nonproj IS MISSING"),
        format!(
            "SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND nonproj IS NOT MISSING"
        ),
        format!(
            "SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND BEGINS_WITH(nonproj, 'N')"
        ),
    ] {
        let msg = error(&db, &sql);
        assert!(
            msg.contains("does not project one or more filter attributes"),
            "for {sql}: got {msg}"
        );
    }
}

#[test]
fn a_negated_filter_on_an_unprojected_attribute_is_rejected_when_keyed() {
    // Captured eu-west-2 2026-08-17. A negation counts as a filter for this
    // rule: the keyed GSI and the keyed LSI both come back with the same
    // rejection the positive filter gets, word for word.
    //
    // It has to. A negation is true of a row whose attribute is missing, and on
    // an index that does not carry the attribute every row looks missing, so
    // serving the read would return exactly the rows the base item contradicts.
    let db = seeded();
    for (index, sql) in [
        (
            "gsi-keys",
            format!(
                "SELECT pk FROM \"{TABLE}\".\"gsi-keys\" WHERE gsiPk2='x2' AND NOT nonproj='N1'"
            ),
        ),
        (
            "lsi-keys",
            format!("SELECT pk FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p' AND NOT nonproj='N1'"),
        ),
    ] {
        let msg = error(&db, &sql);
        assert!(
            msg.contains(&format!(
                "One or more parameter values were invalid: Secondary index {index} \
                 does not project one or more filter attributes: [nonproj]"
            )),
            "for {sql}: got {msg}"
        );
    }
}

#[test]
fn a_negated_key_equality_is_not_pushed_down_as_a_key() {
    // `NOT pk = 'p'` is a filter, not a key condition. Nothing may read the
    // comparison out from under the negation and push it down: a keyed read
    // would go straight to the one row the statement excludes and return it,
    // and on an index it would also count as keying the index. Both come back
    // as scans.
    let db = seeded();

    // Every fixture row shares `pk='p'`, so a negated key equality on the table
    // matches nothing, and a key lookup would have returned all four.
    assert!(
        keys(&db, &format!("SELECT * FROM \"{TABLE}\" WHERE NOT pk='p'")).is_empty(),
        "a negated key equality must not become a key lookup"
    );
    // `sk` distinguishes them, so this one names the rows that are left.
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\" WHERE pk='p' AND NOT sk='s1'")
        ),
        vec!["p/s2", "p/s3", "p/s4"]
    );

    // The same against an index. `gsi-all` holds the three rows carrying
    // `gsiPk`, two of them under `x`, so negating the index key leaves the one
    // under `y`.
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"gsi-all\" WHERE NOT gsiPk='x'")
        ),
        vec!["p/s2"]
    );
}

#[test]
fn a_negated_filter_on_an_unprojected_attribute_is_served_when_unkeyed() {
    // Captured eu-west-2 2026-08-17. The keyed/unkeyed distinction survives the
    // negation: without a condition on the index partition key the read is a
    // scan, and AWS served it, returning every row the index carries.
    let db = seeded();
    let sql = format!("SELECT pk FROM \"{TABLE}\".\"gsi-keys\" WHERE NOT nonproj='N1'");
    // Three of the four fixture rows carry `gsiPk2`, so three is the whole
    // index, which is what a filter no row can contradict should return.
    assert_eq!(keys(&db, &sql).len(), 3, "an unkeyed read is a scan");
}

#[test]
fn a_non_equality_predicate_on_a_projected_attribute_is_accepted() {
    // R7. The control: the same predicate shape on an attribute the index does
    // carry runs and matches nothing.
    let db = seeded();
    assert!(
        keys(
            &db,
            &format!(
                "SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND projattr IS MISSING"
            )
        )
        .is_empty()
    );
}

#[test]
fn an_empty_table_component_is_rejected_the_same_way_as_an_empty_index() {
    // R1, R2. The empty-component rejection was captured on the index half
    // first; AWS applies it to the table half too, ahead of resolving the table.
    let db = seeded();
    for sql in ["SELECT * FROM \"\"", "SELECT * FROM \"\".\"gsi-all\""] {
        let msg = error(&db, sql);
        assert!(
            msg.contains("Path component cannot be an empty string"),
            "for {sql}: got {msg}"
        );
    }
}

#[test]
fn a_keyed_read_accepts_a_filter_the_index_does_carry() {
    // Q36, Q38. The key condition is present but every filter attribute is
    // projected, so there is nothing to reject.
    let db = seeded();
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"gsi-inc\" WHERE gsiPk2='x2' AND projattr='P1'")
        ),
        vec!["p/s1"]
    );
    assert_eq!(
        keys(
            &db,
            &format!("SELECT * FROM \"{TABLE}\".\"lsi-all\" WHERE pk='p' AND lsiSk='l1'")
        ),
        vec!["p/s1"]
    );
}

#[test]
fn an_index_key_is_always_carried_whatever_the_projection_says() {
    // A KEYS_ONLY index still carries its own key and the table's, so
    // filtering and projecting on those is never rejected.
    let db = seeded();
    assert_eq!(
        attributes(
            &db,
            &format!("SELECT pk, sk, lsiSk2 FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p'")
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

#[test]
fn an_lsi_serves_an_unprojected_projection_from_the_base_table() {
    // Q27. An LSI shares its partition with the table, so DynamoDB reads the
    // base item rather than rejecting. dynoxide used to return rows of {}.
    let db = seeded();
    assert_eq!(
        attributes(
            &db,
            &format!("SELECT projattr FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p'")
        ),
        vec!["projattr"]
    );
}

#[test]
fn an_lsi_reach_back_splits_capacity_between_the_arms() {
    // Q27: three rows served this way reported total 2, table 1.5, lsi 0.5.
    // The base fetches land on the table arm at read granularity apiece; the
    // index arm covers the index read alone.
    let db = seeded();
    let cap = capacity(
        &db,
        &format!("SELECT projattr FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p'"),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(1.5));
    assert_eq!(
        cap.local_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("lsi-keys"))
            .map(|d| d.capacity_units),
        Some(0.5)
    );
    assert_eq!(cap.capacity_units, 2.0);
}

#[test]
fn a_projection_the_lsi_does_carry_reads_no_base_items() {
    // The control: without a reach-back the table arm stays at zero.
    let db = seeded();
    let cap = capacity(
        &db,
        &format!("SELECT pk, lsiSk2 FROM \"{TABLE}\".\"lsi-keys\" WHERE pk='p'"),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(0.0));
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
fn the_path_rejections_are_reported_without_the_well_formed_envelope() {
    // Q14, Q15, Q22, Q25. DynamoDB wraps a malformed statement in "Statement
    // wasn't well formed, can't be processed: " and reports these four on their
    // own terms. Which envelope a message takes is observable, so each is
    // pinned rather than just its text.
    let db = seeded();
    for (sql, expected) in [
        (
            format!("SELECT * FROM \"{TABLE}\".\"gsi-all\".\"more\""),
            "A path may contain at most 2 components in the FROM clause",
        ),
        (
            format!("SELECT * FROM \"{TABLE}\".\"\""),
            "Path component cannot be an empty string",
        ),
        (
            format!("INSERT INTO \"{TABLE}\".\"gsi-all\" VALUE {{'pk':'w','sk':'w'}}"),
            "FROM clause may only contain a single table name in data manipulation statements",
        ),
    ] {
        let msg = error(&db, &sql);
        assert!(msg.contains(expected), "for {sql}: got {msg}");
        assert!(
            !msg.contains("wasn't well formed"),
            "for {sql}: wrapped when AWS returns it bare: {msg}"
        );
    }
}

#[test]
fn an_unterminated_quote_is_a_well_formed_envelope_rejection() {
    // R16, R17, R18. DynamoDB returns the envelope with no detail after it. The
    // outcome is what is pinned here, the type and the rejection, because that
    // is the half a caller can depend on. dynoxide names the fault as well,
    // which is additive: AWS's validation prose differed across two of four
    // regions in the 2026-06 capture, so the wording was never the contract.
    let db = seeded();
    for sql in [
        "SELECT * FROM \"",
        "SELECT * FROM \"abc",
        "SELECT * FROM \"pq_idxq\" WHERE pk = 'oops",
    ] {
        let msg = error(&db, sql);
        assert!(
            msg.starts_with("Statement wasn't well formed, can't be processed:"),
            "for {sql}: got {msg}"
        );
    }
}

#[test]
fn a_malformed_statement_still_gets_the_well_formed_envelope() {
    // The control for the test above: an actual syntax error keeps the wrapper.
    let db = seeded();
    let msg = error(&db, "SELECT * FROM");
    assert!(msg.contains("wasn't well formed"), "got {msg}");
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
fn an_index_token_stripped_of_its_base_key_is_rejected() {
    // The fingerprint is copied verbatim from a legitimate token, so only the
    // base-key halves are missing. Without an explicit check the read falls
    // back to a two-column cursor and ends the walk after one row instead of
    // erroring.
    use base64::Engine;
    let db = seeded();
    let sql = format!("SELECT * FROM \"{TABLE}\".\"gsi-all\"");
    let first = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({"Statement": sql, "Limit": 1})).unwrap(),
        )
        .unwrap();
    let token = first
        .next_token
        .expect("a limited read owes a continuation");

    let raw = base64::engine::general_purpose::STANDARD
        .decode(&token)
        .unwrap();
    let mut payload: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    payload["bpk"] = serde_json::Value::Null;
    payload["bsk"] = serde_json::Value::Null;
    let tampered = base64::engine::general_purpose::STANDARD.encode(payload.to_string().as_bytes());

    let msg = db
        .execute_statement(
            serde_json::from_value(serde_json::json!({
                "Statement": sql, "Limit": 1, "NextToken": tampered
            }))
            .unwrap(),
        )
        .expect_err("a truncated index token is rejected")
        .to_string();
    assert!(msg.contains("Invalid NextToken"), "got {msg}");
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

// --- what an LSI reach-back costs, by item size ---------------------------
//
// Captured eu-west-2 2026-08-17. The reach-back used to be charged a flat 0.5
// per row whatever the base item weighed and whatever consistency was asked
// for, which is right only for items that fit in one read block.

const BIG_TABLE: &str = "pq_idxq_big";

/// Three rows under one partition, each carrying a ~9KB attribute the LSI does
/// not project, so a select naming it has to read the base item back.
fn big_items() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(
        serde_json::from_value(serde_json::json!({
            "TableName": BIG_TABLE,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "lsiSk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "LocalSecondaryIndexes": [{
                "IndexName": "lsi-inc",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsiSk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["projattr"]}
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    for n in 1..=3 {
        db.put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": BIG_TABLE,
                "Item": {
                    "pk": {"S": "p"},
                    "sk": {"S": format!("s{n}")},
                    "lsiSk": {"S": format!("l{n}")},
                    "projattr": {"S": format!("P{n}")},
                    // Just over two read blocks, so each row rounds to three.
                    "nonproj": {"S": "x".repeat(9000)}
                }
            }))
            .unwrap(),
        )
        .unwrap();
    }
    db
}

#[test]
fn a_reach_back_over_large_items_is_charged_on_their_bytes() {
    // Three ~9KB rows report table 4.5, not the 1.5 a flat per-row rate gives.
    // Each base read rounds on its own bytes (three blocks apiece) and the
    // charges are summed, which is why this is not 3.5 either: that is what the
    // same rows cost read straight from the table, where the bytes are summed
    // before rounding.
    let db = big_items();
    let cap = capacity_in(
        &db,
        serde_json::json!({
            "Statement": format!("SELECT nonproj FROM \"{BIG_TABLE}\".\"lsi-inc\" WHERE pk='p'"),
            "ReturnConsumedCapacity": "INDEXES"
        }),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(4.5));
    assert_eq!(
        cap.local_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("lsi-inc"))
            .map(|d| d.capacity_units),
        Some(0.5)
    );
}

#[test]
fn a_reach_back_follows_the_requests_consistency() {
    // The same three rows under ConsistentRead report table 9, twice the
    // eventual rate, so the flag reaches the base reads as well as the index
    // read.
    let db = big_items();
    let cap = capacity_in(
        &db,
        serde_json::json!({
            "Statement": format!("SELECT nonproj FROM \"{BIG_TABLE}\".\"lsi-inc\" WHERE pk='p'"),
            "ConsistentRead": true,
            "ReturnConsumedCapacity": "INDEXES"
        }),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(9.0));
    assert_eq!(
        cap.local_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("lsi-inc"))
            .map(|d| d.capacity_units),
        Some(1.0)
    );
}

#[test]
fn a_reach_back_is_charged_on_rows_walked_not_rows_kept() {
    // Captured eu-west-2 2026-08-17. A filter matching none of the three rows
    // returns nothing and still reports table 4.5, the same as the unfiltered
    // read of the same rows. The reach-back happens on the way past a row, so
    // filtering the row out afterwards refunds nothing.
    let db = big_items();
    let cap = capacity_in(
        &db,
        serde_json::json!({
            "Statement": format!(
                "SELECT nonproj FROM \"{BIG_TABLE}\".\"lsi-inc\" WHERE pk='p' AND projattr='absent'"
            ),
            "ReturnConsumedCapacity": "INDEXES"
        }),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(4.5));
    assert_eq!(
        cap.local_secondary_indexes
            .as_ref()
            .and_then(|m| m.get("lsi-inc"))
            .map(|d| d.capacity_units),
        Some(0.5)
    );
}

#[test]
fn select_star_against_an_include_index_does_not_reach_back() {
    // The control, and a surprise worth pinning: `SELECT *` names no attribute
    // the index fails to project, so it is served from the index alone and the
    // table arm stays at zero however large the base items are. Captured at
    // total 0.5.
    let db = big_items();
    let cap = capacity_in(
        &db,
        serde_json::json!({
            "Statement": format!("SELECT * FROM \"{BIG_TABLE}\".\"lsi-inc\" WHERE pk='p'"),
            "ReturnConsumedCapacity": "INDEXES"
        }),
    );
    assert_eq!(cap.table.as_ref().map(|t| t.capacity_units), Some(0.0));
    assert_eq!(cap.capacity_units, 0.5);

    // And the rows say the same thing the capacity does. A zero table arm only
    // means no reach-back if the attributes it would have fetched are absent,
    // so the projected set is what comes back and `nonproj` is not in it.
    assert_eq!(
        attributes(
            &db,
            &format!("SELECT * FROM \"{BIG_TABLE}\".\"lsi-inc\" WHERE pk='p'")
        ),
        vec!["lsiSk", "pk", "projattr", "sk"]
    );
}
