//! Operands of a PartiQL WHERE comparison beyond `path op value`.
//!
//! DynamoDB evaluates `size(path)` and `attribute_type(path, type)` in a WHERE
//! clause, takes a literal or a parameter on either side of a comparison, and
//! compares two paths or two literals. A comparison with a missing value, or
//! with the size of one that has none, is false for `=` and the ordering
//! operators and true for `<>`. `MISSING` can only be tested with `IS`, a
//! condition function is a boolean that the ordering operators refuse, and a
//! size on its own is not a condition. All of these are refused before the
//! table is looked up (captured eu-west-2 and us-east-1, 2026-10-09; both
//! regions answered every case identically).

use dynoxide::Database;
use dynoxide::actions::batch_execute_statement::{
    BatchExecuteStatementRequest, BatchStatementRequest,
};
use dynoxide::actions::create_table::CreateTableRequest;
use dynoxide::actions::execute_statement::{ExecuteStatementRequest, ExecuteStatementResponse};
use dynoxide::actions::execute_transaction::{ExecuteTransactionRequest, ParameterizedStatement};
use dynoxide::actions::get_item::GetItemRequest;
use dynoxide::actions::put_item::PutItemRequest;
use dynoxide::errors::DynoxideError;
use dynoxide::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
};
use std::collections::HashMap;

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

/// The item the captures used: one attribute of each type, a string with a
/// two-byte character and one outside the Basic Multilingual Plane.
fn rich(pk: &str) -> Item {
    HashMap::from([
        ("pk".to_string(), s(pk)),
        ("s".to_string(), s("str")),
        ("s2".to_string(), s("h\u{e9}llo")),
        ("s3".to_string(), s("\u{1F600}")),
        ("e".to_string(), s("")),
        ("n".to_string(), n("5")),
        ("b".to_string(), AttributeValue::B(vec![1, 2, 3, 4])),
        (
            "ss".to_string(),
            AttributeValue::SS(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ),
        (
            "ns".to_string(),
            AttributeValue::NS(vec!["1".to_string(), "2".to_string(), "3".to_string()]),
        ),
        ("bs".to_string(), AttributeValue::BS(vec![vec![1], vec![2]])),
        ("l".to_string(), AttributeValue::L(vec![s("x"), s("y")])),
        (
            "m".to_string(),
            AttributeValue::M(HashMap::from([
                ("sub".to_string(), s("x")),
                ("other".to_string(), n("1")),
            ])),
        ),
        ("nul".to_string(), AttributeValue::NULL(true)),
        ("flag".to_string(), AttributeValue::BOOL(true)),
        ("ty".to_string(), s("S")),
    ])
}

fn db() -> Database {
    let db = Database::memory().unwrap();
    db.create_table(CreateTableRequest {
        table_name: "Things".to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::HASH,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        ..Default::default()
    })
    .unwrap();
    for pk in ["it1", "it2", "it3", "it4"] {
        db.put_item(PutItemRequest {
            table_name: "Things".to_string(),
            item: rich(pk),
            ..Default::default()
        })
        .unwrap();
    }
    db
}

fn run(
    db: &Database,
    statement: &str,
    parameters: Option<Vec<AttributeValue>>,
) -> Result<ExecuteStatementResponse, DynoxideError> {
    db.execute_statement(ExecuteStatementRequest {
        statement: statement.to_string(),
        parameters,
        ..Default::default()
    })
}

/// The keys `SELECT pk ... WHERE pk = 'it1' AND <cond>` returns.
fn keys_where(db: &Database, condition: &str) -> Vec<String> {
    keys(
        db,
        &format!(r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND {condition}"#),
        None,
    )
}

fn keys(db: &Database, statement: &str, parameters: Option<Vec<AttributeValue>>) -> Vec<String> {
    let response = run(db, statement, parameters)
        .unwrap_or_else(|e| panic!("{statement}: expected rows, got {e:?}"));
    let mut out: Vec<String> = response
        .items
        .unwrap_or_default()
        .into_iter()
        .map(|item| match item.get("pk") {
            Some(AttributeValue::S(pk)) => pk.clone(),
            other => panic!("{statement}: no key in {other:?}"),
        })
        .collect();
    out.sort();
    out
}

fn refusal(db: &Database, statement: &str) -> String {
    refusal_with(db, statement, None)
}

fn refusal_with(db: &Database, statement: &str, parameters: Option<Vec<AttributeValue>>) -> String {
    match run(db, statement, parameters) {
        Err(DynoxideError::ValidationException(m)) => m,
        other => panic!("{statement}: expected a ValidationException, got {other:?}"),
    }
}

fn where_refusal(db: &Database, condition: &str) -> String {
    refusal(
        db,
        &format!(r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND {condition}"#),
    )
}

fn stored(db: &Database, pk: &str) -> Option<Item> {
    db.get_item(GetItemRequest {
        table_name: "Things".to_string(),
        key: HashMap::from([("pk".to_string(), s(pk))]),
        ..Default::default()
    })
    .unwrap()
    .item
}

const IT1: &[&str] = &["it1"];
const NONE: &[&str] = &[];

#[test]
fn size_measures_strings_binaries_and_collections() {
    let db = db();
    for (condition, expected) in [
        ("size(s) = 3", IT1),
        ("size(s) = 4", NONE),
        // Characters as Java counts them: UTF-16 code units.
        ("size(s2) = 5", IT1),
        ("size(s2) = 6", NONE),
        ("size(s3) = 2", IT1),
        ("size(s3) = 1", NONE),
        ("size(e) = 0", IT1),
        ("size(b) = 4", IT1),
        ("size(ss) = 3", IT1),
        ("size(ns) = 3", IT1),
        ("size(bs) = 2", IT1),
        ("size(l) = 2", IT1),
        ("size(m) = 2", IT1),
        ("size(m.sub) = 1", IT1),
        ("size(l[0]) = 1", IT1),
        ("SIZE(s) = 3", IT1),
        ("size(s) = 3.0", IT1),
        ("size(s) = '3'", NONE),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
}

#[test]
fn size_takes_every_comparison_and_either_side() {
    let db = db();
    for (condition, expected) in [
        ("size(s) > 2", IT1),
        ("size(s) < 4", IT1),
        ("size(s) >= 3", IT1),
        ("size(s) <= 2", NONE),
        ("size(s) <> 4", IT1),
        ("size(s) != 4", IT1),
        ("size(s) BETWEEN 1 AND 5", IT1),
        ("size(s) IN [3, 4]", IT1),
        ("3 = size(s)", IT1),
        ("size(s) = size(ss)", IT1),
        ("size(s) = n", NONE),
        ("NOT size(s) = 3", NONE),
        ("NOT (size(s) = 4)", IT1),
        ("(size(s) = 4 OR size(ss) = 3)", IT1),
        ("size('abc') = 3", IT1),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
    assert_eq!(
        keys(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND size(s) = ?"#,
            Some(vec![n("3")])
        ),
        IT1
    );
    assert_eq!(
        keys(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND size(?) = 3"#,
            Some(vec![s("abc")])
        ),
        IT1
    );
    // With no key condition it filters a scan.
    assert_eq!(
        keys(&db, r#"SELECT pk FROM "Things" WHERE size(s) = 3"#, None),
        ["it1", "it2", "it3", "it4"]
    );
}

#[test]
fn a_value_with_no_size_compares_like_a_missing_one() {
    let db = db();
    for (condition, expected) in [
        ("size(n) = 1", NONE),
        ("size(flag) = 1", NONE),
        ("size(nul) = 1", NONE),
        ("size(n) <> 1", IT1),
        ("size(flag) <> 1", IT1),
        ("size(nul) <> 1", IT1),
        ("size(n) > 0", NONE),
        ("size(n) IN [1, 5]", NONE),
        ("size(nope) = 0", NONE),
        ("size(nope) <> 0", IT1),
        ("size(nope) > 0", NONE),
        ("size(nope) < 1", NONE),
        ("NOT size(nope) = 0", IT1),
        ("size(nope) BETWEEN 0 AND 9", NONE),
        ("size(nope) IN [0, 1]", NONE),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
}

#[test]
fn a_missing_attribute_is_unequal_to_anything() {
    let db = db();
    for (condition, expected) in [
        ("nope <> 'x'", IT1),
        ("nope = 'x'", NONE),
        ("nope < 'x'", NONE),
        ("NOT nope = 'x'", IT1),
        ("NOT nope <> 'x'", NONE),
        ("n <> 'x'", IT1),
        ("n < 'x'", NONE),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
}

#[test]
fn size_is_refused_where_it_cannot_stand() {
    let db = db();
    assert_eq!(
        where_refusal(&db, "size(s, ss) = 3"),
        "Incorrect number of operands for operator or function; operator or function: size, number of operands: 2"
    );
    assert_eq!(
        where_refusal(&db, "size() = 3"),
        "Incorrect number of operands for operator or function; operator or function: size, number of operands: 0"
    );
    assert_eq!(
        where_refusal(&db, "size(s)"),
        "Incorrect operand type for operator or function; operator or function: AND, operand type: N"
    );
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Things" WHERE size(s) OR pk = 'it1'"#
        ),
        "Incorrect operand type for operator or function; operator or function: OR, operand type: N"
    );
    assert_eq!(
        where_refusal(&db, "NOT size(s)"),
        "Incorrect operand type for operator or function; operator or function: NOT, operand type: N"
    );
    assert_eq!(
        refusal(&db, r#"SELECT pk FROM "Things" WHERE size(s)"#),
        "WHERE clause must contain an expression that resolves to a boolean"
    );
    assert_eq!(
        where_refusal(&db, "size(s) + 1 = 4"),
        "Unsupported operator in Condition Expression. Operator: +"
    );
    assert_eq!(
        where_refusal(&db, "size(nope) IS MISSING"),
        "The left hand side argument to IS must be a Variable Reference, Literal, or Path"
    );
    // Before the table is looked up.
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Missing" WHERE pk = 'it1' AND size() = 1"#
        ),
        "Incorrect number of operands for operator or function; operator or function: size, number of operands: 0"
    );
}

#[test]
fn size_conditions_a_write() {
    let db = db();
    run(
        &db,
        r#"UPDATE "Things" SET z = 1 WHERE pk = 'it3' AND size(s) = 3"#,
        None,
    )
    .unwrap();
    assert_eq!(stored(&db, "it3").unwrap().get("z"), Some(&n("1")));
    let refused = run(
        &db,
        r#"UPDATE "Things" SET z = 2 WHERE pk = 'it3' AND size(s) = 4"#,
        None,
    );
    assert!(
        matches!(
            refused,
            Err(DynoxideError::ConditionalCheckFailedException(..))
        ),
        "{refused:?}"
    );
    assert_eq!(stored(&db, "it3").unwrap().get("z"), Some(&n("1")));
    let refused = run(
        &db,
        r#"DELETE FROM "Things" WHERE pk = 'it4' AND size(s) = 9"#,
        None,
    );
    assert!(
        matches!(
            refused,
            Err(DynoxideError::ConditionalCheckFailedException(..))
        ),
        "{refused:?}"
    );
    // A transaction SELECT holds to the primary key.
    let refused = db.execute_transaction(ExecuteTransactionRequest {
        transact_statements: vec![ParameterizedStatement {
            statement: r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND size(s) = 3"#.to_string(),
            parameters: None,
            ..Default::default()
        }],
        ..Default::default()
    });
    match refused {
        Err(DynoxideError::ValidationException(m)) => assert_eq!(
            m,
            "Validation failed in TransactStatements[0]: Select statements within ExecuteTransaction must specify the primary key in the where clause."
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn attribute_type_tests_a_value_against_dynamodb_type_names() {
    let db = db();
    for (condition, expected) in [
        ("attribute_type(ss, 'SS')", IT1),
        ("attribute_type(ss, 'S')", NONE),
        ("attribute_type(ss, 'L')", NONE),
        ("attribute_type(s, 'S')", IT1),
        ("attribute_type(n, 'N')", IT1),
        ("attribute_type(b, 'B')", IT1),
        ("attribute_type(flag, 'BOOL')", IT1),
        ("attribute_type(nul, 'NULL')", IT1),
        ("attribute_type(l, 'L')", IT1),
        ("attribute_type(m, 'M')", IT1),
        ("attribute_type(ns, 'NS')", IT1),
        ("attribute_type(bs, 'BS')", IT1),
        ("attribute_type(m.sub, 'S')", IT1),
        ("attribute_type(nope, 'S')", NONE),
        ("attribute_type('s', 'S')", IT1),
        ("attribute_type(size(s), 'N')", IT1),
        ("attribute_type(s, 'S') = true", IT1),
        ("attribute_type(s, 'S') = false", NONE),
        ("attribute_type(s, 'S') <> 1", IT1),
        ("NOT attribute_type(s, 'N')", IT1),
        ("ATTRIBUTE_TYPE(s, 'S')", IT1),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
    assert_eq!(
        keys(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND attribute_type(s, ?)"#,
            Some(vec![s("S")])
        ),
        IT1
    );
    run(
        &db,
        r#"UPDATE "Things" SET z = 4 WHERE pk = 'it3' AND attribute_type(s, 'S')"#,
        None,
    )
    .unwrap();
    let refused = run(
        &db,
        r#"UPDATE "Things" SET z = 5 WHERE pk = 'it3' AND attribute_type(s, 'N')"#,
        None,
    );
    assert!(
        matches!(
            refused,
            Err(DynoxideError::ConditionalCheckFailedException(..))
        ),
        "{refused:?}"
    );
}

#[test]
fn attribute_type_refuses_what_it_cannot_take() {
    let db = db();
    let valid = "valid types: {N,BS,L,B,NULL,M,S,SS,NS,BOOL}";
    for name in ["s", "X", "STRING"] {
        assert_eq!(
            where_refusal(&db, &format!("attribute_type(s, '{name}')")),
            format!("Invalid attribute type name found; type: {name}, {valid}")
        );
    }
    assert_eq!(
        refusal_with(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND attribute_type(s, ?)"#,
            Some(vec![s("Q")])
        ),
        format!("Invalid attribute type name found; type: Q, {valid}")
    );
    let operand_n = "Incorrect operand type for operator or function; operator or function: attribute_type, operand type: N";
    assert_eq!(where_refusal(&db, "attribute_type(s, 1)"), operand_n);
    assert_eq!(
        refusal_with(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND attribute_type(s, ?)"#,
            Some(vec![n("1")])
        ),
        operand_n
    );
    assert_eq!(
        where_refusal(&db, "attribute_type(s)"),
        "Incorrect number of operands for operator or function; operator or function: attribute_type, number of operands: 1"
    );
    assert_eq!(
        where_refusal(&db, "attribute_type(s, 'S', 'N')"),
        "Incorrect number of operands for operator or function; operator or function: attribute_type, number of operands: 3"
    );
    assert_eq!(
        where_refusal(&db, "attribute_type(s, s)"),
        "The first operand must be distinct from the remaining operands for this operator or function; operator: attribute_type, first operand: [s]"
    );
    // A path as the type: DynamoDB lists the operand types it would take, in
    // the order of a Java hash set, which differs between hosts; this is the
    // eu-west-2 order.
    assert_eq!(
        where_refusal(&db, "attribute_type(s, ty)"),
        "Incorrect operand type for operator or function; operator or function: attribute_type, operand type: {S,HDS,N,FS,BS,NULL,DOUBLE,NS,DECIMAL,SS,DECIMALSET,BOOL,HD,DICT,INTSET,M,L,B,FLOAT,INT,DOUBLESET}"
    );
    assert_eq!(
        where_refusal(&db, "attribute_type(s, 'S') < true"),
        "Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"
    );
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Missing" WHERE pk = 'it1' AND attribute_type(s, 'X')"#
        ),
        format!("Invalid attribute type name found; type: X, {valid}")
    );
}

#[test]
fn a_literal_on_the_left_compares_as_its_mirror() {
    let db = db();
    assert_eq!(
        keys(&db, r#"SELECT pk FROM "Things" WHERE 'it1' = pk"#, None),
        IT1
    );
    for (condition, expected) in [
        ("5 = n", IT1),
        ("4 < n", IT1),
        ("6 > n", IT1),
        ("5 <= n", IT1),
        ("5 >= n", IT1),
        ("4 <> n", IT1),
        ("5 < n", NONE),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
    assert_eq!(
        keys(
            &db,
            r#"SELECT pk FROM "Things" WHERE ? = pk"#,
            Some(vec![s("it1")])
        ),
        IT1
    );
}

#[test]
fn a_literal_on_the_left_names_the_item_a_write_changes() {
    let db = db();
    run(&db, r#"UPDATE "Things" SET z = 7 WHERE 'it3' = pk"#, None).unwrap();
    assert_eq!(stored(&db, "it3").unwrap().get("z"), Some(&n("7")));
    run(
        &db,
        r#"UPDATE "Things" SET z = 8 WHERE 'it3' = pk AND 'str' = s"#,
        None,
    )
    .unwrap();
    assert_eq!(stored(&db, "it3").unwrap().get("z"), Some(&n("8")));
    let refused = run(
        &db,
        r#"UPDATE "Things" SET z = 9 WHERE 'it3' = pk AND 'nope' = s"#,
        None,
    );
    assert!(
        matches!(
            refused,
            Err(DynoxideError::ConditionalCheckFailedException(..))
        ),
        "{refused:?}"
    );
    run(&db, r#"FROM "Things" WHERE 'it3' = pk SET z = 10"#, None).unwrap();
    assert_eq!(stored(&db, "it3").unwrap().get("z"), Some(&n("10")));
    run(&db, r#"DELETE FROM "Things" WHERE 'it4' = pk"#, None).unwrap();
    assert!(stored(&db, "it4").is_none());

    // Inside a transaction and a batch, it is the primary key.
    let response = db
        .execute_transaction(ExecuteTransactionRequest {
            transact_statements: vec![ParameterizedStatement {
                statement: r#"SELECT pk FROM "Things" WHERE 'it1' = pk"#.to_string(),
                parameters: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
    let responses = response.responses.unwrap_or_default();
    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0].item.as_ref().and_then(|i| i.get("pk")),
        Some(&s("it1"))
    );
    let mut member = BatchStatementRequest::default();
    member.statement = r#"SELECT pk FROM "Things" WHERE 'it1' = pk"#.to_string();
    let response = db
        .batch_execute_statement(BatchExecuteStatementRequest {
            statements: vec![member],
            ..Default::default()
        })
        .unwrap();
    assert!(
        response.responses[0].error.is_none(),
        "{:?}",
        response.responses[0]
    );
    assert_eq!(
        response.responses[0]
            .item
            .as_ref()
            .and_then(|i| i.get("pk")),
        Some(&s("it1"))
    );
}

#[test]
fn two_literals_or_two_paths_compare() {
    let db = db();
    assert_eq!(keys_where(&db, "'a' = 'a'"), IT1);
    assert_eq!(keys_where(&db, "'a' = 'b'"), NONE);
    assert_eq!(keys_where(&db, "s = s2"), NONE);
    assert_eq!(keys_where(&db, "s <> s2"), IT1);
    assert_eq!(
        where_refusal(&db, "s = s"),
        "The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [s]"
    );
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Missing" WHERE pk = 'it1' AND s = s"#
        ),
        "The first operand must be distinct from the remaining operands for this operator or function; operator: =, first operand: [s]"
    );
}

/// IN and BETWEEN refuse the path they test among their own members or
/// bounds, wherever it stands, as a comparison refuses `s = s`. A path is
/// named by its parts: `m.a` as `[m, a]` and `l[0]` as `[l, [0]]`, in a
/// comparison too. Only a path is checked, by name as written (a quoted name
/// is the same path, another case isn't). The first refusal in the clause is
/// the one reported, and it comes before the table is looked up and before an
/// IN with too many operands later in the clause (captured eu-west-2 and
/// us-east-1, 2026-10-10).
#[test]
fn in_and_between_refuse_the_path_among_their_own_operands() {
    let db = db();
    let distinct = |operator: &str, first: &str| {
        format!(
            "The first operand must be distinct from the remaining operands for this operator \
             or function; operator: {operator}, first operand: [{first}]"
        )
    };
    let long_in = (0..101)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    for (condition, operator, first) in [
        ("s IN [s]".to_string(), "IN", "s"),
        ("s IN ['a', s]".to_string(), "IN", "s"),
        ("s IN [s, 'a']".to_string(), "IN", "s"),
        ("s NOT IN [s]".to_string(), "IN", "s"),
        ("NOT s IN [s]".to_string(), "IN", "s"),
        (r#""s" IN [s]"#.to_string(), "IN", "s"),
        ("s BETWEEN s AND 'z'".to_string(), "BETWEEN", "s"),
        ("s BETWEEN 'a' AND s".to_string(), "BETWEEN", "s"),
        ("s BETWEEN s AND s".to_string(), "BETWEEN", "s"),
        ("m.sub IN [m.sub]".to_string(), "IN", "m, sub"),
        (
            "m.sub BETWEEN m.sub AND 'z'".to_string(),
            "BETWEEN",
            "m, sub",
        ),
        ("l[0] IN [l[0]]".to_string(), "IN", "l, [0]"),
        ("m.sub = m.sub".to_string(), "=", "m, sub"),
        ("s IN [s] AND n = n".to_string(), "IN", "s"),
        (format!("s IN [s] AND n IN [{long_in}]"), "IN", "s"),
    ] {
        assert_eq!(
            where_refusal(&db, &condition),
            distinct(operator, first),
            "{condition}"
        );
    }
    assert_eq!(
        refusal(&db, r#"SELECT pk FROM "Missing" WHERE s IN [s]"#),
        distinct("IN", "s")
    );
    // Not the path itself: these run.
    for (condition, expected) in [
        ("s IN [s2, s2]", NONE),
        ("size(s) IN [size(s)]", IT1),
        ("size(s) BETWEEN size(s) AND 9", IT1),
        ("s IN [S]", NONE),
        ("'a' IN ['a']", IT1),
        ("s BETWEEN 'str' AND 'str'", IT1),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
}

/// An IN with more than 100 members that also holds the path it tests is
/// refused as not distinct, not for its operand count, whether the path comes
/// first or last (captured eu-west-2 and us-east-1, 2026-10-10).
#[test]
fn an_in_list_names_its_own_path_before_its_operand_count() {
    let db = db();
    let hundred = (1..=100)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let distinct = "The first operand must be distinct from the remaining operands for this \
                    operator or function; operator: IN, first operand: [n]";
    for condition in [
        format!("n IN [n, {hundred}]"),
        format!("n IN [{hundred}, n]"),
    ] {
        assert_eq!(where_refusal(&db, &condition), distinct, "{condition}");
    }
}

/// Which refusal DynamoDB reports when a WHERE clause holds two. A function
/// it doesn't recognise, a number it can't store and `MISSING` compared
/// without IS outrank the rest, the first written of them winning. Below them,
/// an operand that isn't distinct, an IN with too many operands and an
/// operand of the wrong type are reported in the order written (captured
/// eu-west-2 and us-east-1, 2026-10-10; the regions agree on every row).
#[test]
fn refusals_rank_as_dynamodb_ranks_them() {
    let db = db();
    let long_in = format!(
        "n IN [{}]",
        (0..101)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let in_count = "The IN operator is provided with too many operands; number of operands: 101";
    let distinct = |operator: &str| {
        format!(
            "The first operand must be distinct from the remaining operands for this operator \
             or function; operator: {operator}, first operand: [s]"
        )
    };
    let ordered_bool = "Incorrect operand type for operator or function; operator or function: <, \
                        operand type: BOOL";
    let missing = "MISSING must be compared using IS operator";
    let overflow = "Number overflow. Attempting to store a number with magnitude larger than \
                    supported range under root";
    let unrecognised = "Unrecognized function: foo at ";
    for (condition, expected) in [
        (format!("{long_in} AND s IN [s]"), in_count.to_string()),
        (format!("s = s AND {long_in}"), distinct("=")),
        (format!("{long_in} AND s = s"), in_count.to_string()),
        ("s IN [s] AND size(s) < true".to_string(), distinct("IN")),
        (
            "size(s) < true AND s IN [s]".to_string(),
            ordered_bool.to_string(),
        ),
        (
            format!("size(s) < true AND {long_in}"),
            ordered_bool.to_string(),
        ),
        (
            format!("s BETWEEN s AND 1 AND {long_in}"),
            distinct("BETWEEN"),
        ),
        (
            format!("{long_in} AND s BETWEEN s AND 1"),
            in_count.to_string(),
        ),
        (
            format!("attribute_type(s, s) AND {long_in}"),
            distinct("attribute_type"),
        ),
        (
            format!("{long_in} AND attribute_type(s, s)"),
            in_count.to_string(),
        ),
        (
            format!("{long_in} AND size(s) < true"),
            in_count.to_string(),
        ),
        ("s IN [s] AND x = MISSING".to_string(), missing.to_string()),
        ("x = MISSING AND s IN [s]".to_string(), missing.to_string()),
        (format!("x = MISSING AND {long_in}"), missing.to_string()),
        (format!("{long_in} AND x = MISSING"), missing.to_string()),
        (
            "size(s) < true AND x = MISSING".to_string(),
            missing.to_string(),
        ),
        (
            "x = MISSING AND size(s) < true".to_string(),
            missing.to_string(),
        ),
        ("s = s AND x = MISSING".to_string(), missing.to_string()),
        (
            "s IN [s] AND x = 1e999999999".to_string(),
            overflow.to_string(),
        ),
        (
            "x = 1e999999999 AND s IN [s]".to_string(),
            overflow.to_string(),
        ),
        (
            format!("x = 1e999999999 AND {long_in}"),
            overflow.to_string(),
        ),
        (
            format!("{long_in} AND x = 1e999999999"),
            overflow.to_string(),
        ),
        (
            "x = MISSING AND x = 1e999999999".to_string(),
            missing.to_string(),
        ),
        (
            "x = 1e999999999 AND x = MISSING".to_string(),
            overflow.to_string(),
        ),
        ("x = MISSING AND foo(s)".to_string(), missing.to_string()),
    ] {
        assert_eq!(where_refusal(&db, &condition), expected, "{condition}");
    }
    for condition in [
        "foo(s) AND s IN [s]".to_string(),
        "s IN [s] AND foo(s)".to_string(),
        "s = s AND foo(s)".to_string(),
        format!("foo(s) AND {long_in}"),
        format!("{long_in} AND foo(s)"),
        "foo(s) AND x = MISSING".to_string(),
    ] {
        let message = where_refusal(&db, &condition);
        assert!(message.starts_with(unrecognised), "{condition}: {message}");
    }
}

#[test]
fn missing_is_tested_only_with_is() {
    let db = db();
    let message = "MISSING must be compared using IS operator";
    for condition in [
        "s = missing",
        "s <> MISSING",
        "missing = s",
        "attribute_exists(s) = missing",
        "attribute_exists(s) <> missing",
        "size(s) = missing",
        "s IN [missing]",
        "s BETWEEN missing AND 'z'",
        "missing = missing",
    ] {
        assert_eq!(where_refusal(&db, condition), message, "{condition}");
    }
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Missing" WHERE pk = 'it1' AND s = missing"#
        ),
        message
    );
}

#[test]
fn a_condition_function_is_a_boolean() {
    let db = db();
    for (condition, expected) in [
        ("attribute_exists(s) <> 1", IT1),
        ("attribute_exists(s) <> 'true'", IT1),
        ("attribute_exists(nope) <> 1", IT1),
        ("attribute_exists(s) = 1", NONE),
        ("attribute_exists(s) <> null", IT1),
        ("attribute_exists(s) <> [1]", IT1),
        ("attribute_exists(s) IN [true]", IT1),
        ("attribute_exists(nope) IN [true]", NONE),
    ] {
        assert_eq!(keys_where(&db, condition), expected, "{condition}");
    }
    for (condition, op) in [
        ("attribute_exists(s) > true", ">"),
        ("attribute_exists(s) < 1", "<"),
        ("attribute_exists(s) <= true", "<="),
        ("attribute_exists(s) >= 1", ">="),
        ("attribute_exists(s) BETWEEN true AND true", "BETWEEN"),
    ] {
        assert_eq!(
            where_refusal(&db, condition),
            format!(
                "Incorrect operand type for operator or function; operator or function: {op}, operand type: BOOL"
            ),
            "{condition}"
        );
    }
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Missing" WHERE pk = 'it1' AND attribute_exists(s) > true"#
        ),
        "Incorrect operand type for operator or function; operator or function: >, operand type: BOOL"
    );
    run(
        &db,
        r#"UPDATE "Things" SET z = 11 WHERE pk = 'it3' AND attribute_exists(nope) <> 1"#,
        None,
    )
    .unwrap();
    assert_eq!(stored(&db, "it3").unwrap().get("z"), Some(&n("11")));
}

#[test]
fn a_comparison_with_nothing_after_its_operator_is_unfinished() {
    let db = db();
    for statement in [
        r#"SELECT pk FROM "Things" WHERE pk ="#,
        r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND n <"#,
        r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND n <>;"#,
    ] {
        assert_eq!(
            refusal(&db, statement),
            "Statement wasn't well formed, can't be processed: Missing right-hand side expression of infix operator",
            "{statement}"
        );
    }
    assert_eq!(
        refusal(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND n BETWEEN 1 AND"#
        ),
        "Statement wasn't well formed, can't be processed: Expected expression after AND"
    );
}

/// A size is a number, so it takes the type checks a number does: a boolean
/// under an ordering operator is refused, and BETWEEN's bounds must be in
/// order. A string or a boolean compared with it by `=` is simply false, and
/// by `<>` true. Captured eu-west-2 and us-east-1, 2026-10-10, against an item
/// whose `s` holds three characters, as `rich`'s does.
#[test]
fn size_takes_a_numbers_type_checks() {
    let db = db();
    let operand = |op: &str| {
        format!(
            "Incorrect operand type for operator or function; operator or function: {op}, \
             operand type: BOOL"
        )
    };
    assert_eq!(where_refusal(&db, "size(s) < true"), operand("<"));
    assert_eq!(where_refusal(&db, "size(s) > true"), operand(">"));
    assert_eq!(
        refusal_with(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND size(s) < ?"#,
            Some(vec![AttributeValue::BOOL(true)]),
        ),
        operand("<")
    );
    assert_eq!(
        where_refusal(&db, "size(s) BETWEEN 'b' AND 'a'"),
        "The BETWEEN operator requires upper bound to be greater than or equal to lower \
         bound; lower bound operand: AttributeValue: {S:b}, upper bound operand: \
         AttributeValue: {S:a}"
    );
    assert_eq!(
        where_refusal(&db, "size(s) BETWEEN 10 AND 1"),
        "The BETWEEN operator requires upper bound to be greater than or equal to lower \
         bound; lower bound operand: AttributeValue: {N:10}, upper bound operand: \
         AttributeValue: {N:1}"
    );
    // Inferred, not captured: a NOT in front of either is checked as the
    // condition without it, as other conditions with no negated form of their
    // own are. No capture holds a refused negated size.
    assert_eq!(where_refusal(&db, "NOT size(s) < true"), operand("<"));
    assert_eq!(
        where_refusal(&db, "NOT (size(s) BETWEEN 10 AND 1)"),
        "The BETWEEN operator requires upper bound to be greater than or equal to lower \
         bound; lower bound operand: AttributeValue: {N:10}, upper bound operand: \
         AttributeValue: {N:1}"
    );

    assert_eq!(keys_where(&db, "size(s) = true"), NONE);
    assert_eq!(keys_where(&db, "size(s) < 'x'"), NONE);
    assert_eq!(keys_where(&db, "size(s) <> 'x'"), IT1);
    assert_eq!(keys_where(&db, "size(s) IN ['3']"), NONE);
    assert_eq!(
        keys(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk = 'it1' AND size(s) = ?"#,
            Some(vec![s("3")]),
        ),
        NONE
    );
    assert_eq!(keys_where(&db, "size(s) = 3"), IT1);
    assert_eq!(keys_where(&db, "size(s) BETWEEN 1 AND 10"), IT1);
}

/// A BETWEEN bound or an IN member can be a path or a size, on either side:
/// `size(s2) IN [n]` compares the size with the attribute `n`, not with the
/// string `'n'`, and `n BETWEEN size(s) AND 10` reads its lower bound from the
/// item (captured eu-west-2 and us-east-1, 2026-10-09: size works against a
/// path on either side of BETWEEN and IN). Bounds and members that are all
/// values read as before, keys included.
#[test]
fn between_bounds_and_in_members_can_be_operands() {
    let db = db();
    // `rich`: s = "str" (size 3), s2 = "héllo" (size 5), n = 5.
    assert_eq!(keys_where(&db, "size(s2) IN [n]"), IT1);
    assert_eq!(keys_where(&db, "size(s) IN [n]"), NONE);
    assert_eq!(keys_where(&db, "n IN [size(s2)]"), IT1);
    assert_eq!(keys_where(&db, "n BETWEEN size(s) AND 10"), IT1);
    assert_eq!(keys_where(&db, "n BETWEEN size(s) AND 4"), NONE);
    assert_eq!(keys_where(&db, "size(s) BETWEEN 1 AND n"), IT1);

    assert_eq!(keys_where(&db, "n BETWEEN 1 AND 10"), IT1);
    assert_eq!(keys_where(&db, "n IN [5, 6]"), IT1);
    assert_eq!(keys_where(&db, "n IN [4]"), NONE);
    assert_eq!(
        keys(
            &db,
            r#"SELECT pk FROM "Things" WHERE pk IN ['it1', 'it2']"#,
            None
        ),
        ["it1", "it2"]
    );
}
