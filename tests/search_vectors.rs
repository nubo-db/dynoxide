//! SearchVectors data-plane tests: scoring per distance function, ranking and
//! tie-breaks, TopK bounds, SearchConditionExpression handling, projection,
//! and the captured error strings.
//!
//! Error strings and score semantics are pinned byte-for-byte to real
//! DynamoDB behaviour captured in eu-west-2 on 2026-08-11, with follow-up
//! captures on 2026-08-12 and 2026-08-13 that were byte-identical in
//! eu-west-2 and us-east-1. Elements are f32 (the index's storage precision)
//! but scores accumulate in f64 and saturate to the f32 range: extremal
//! scores are asserted exactly, intermediate ones within a tolerance that
//! absorbs floating-point rounding.

use dynoxide::actions::search_vectors::{SearchVectorsRequest, SearchVectorsResponse};
use dynoxide::storage::Storage;
use serde_json::json;

async fn create_table(storage: &Storage, req: serde_json::Value) {
    let req: dynoxide::actions::create_table::CreateTableRequest =
        serde_json::from_value(req).unwrap();
    dynoxide::actions::create_table::execute(storage, req)
        .await
        .unwrap();
}

async fn put_item(storage: &Storage, table: &str, item: serde_json::Value) {
    let req = serde_json::from_value(json!({"TableName": table, "Item": item})).unwrap();
    dynoxide::actions::put_item::execute(storage, req)
        .await
        .unwrap();
}

async fn search(
    storage: &Storage,
    req: serde_json::Value,
) -> dynoxide::Result<SearchVectorsResponse> {
    let req: SearchVectorsRequest = serde_json::from_value(req).unwrap();
    dynoxide::actions::search_vectors::execute(storage, req).await
}

/// Unwrap a request-model rejection raised inside serde deserialisation.
/// The raw serde error carries the internal `VALIDATION:` marker that the
/// server layer strips before anything reaches the wire; strip it here so
/// assertions pin the full client-visible message.
fn request_model_error(result: Result<SearchVectorsRequest, serde_json::Error>) -> String {
    let err = result.unwrap_err().to_string();
    err.strip_prefix("VALIDATION:")
        .expect("request-model rejections carry the VALIDATION: marker")
        .to_string()
}

/// The `(pk, score)` pairs of a response, in response order.
fn scores(resp: &SearchVectorsResponse) -> Vec<(String, f64)> {
    resp.search_results
        .iter()
        .map(|r| {
            let pk = match r.item.get("pk") {
                Some(dynoxide::types::AttributeValue::S(s)) => s.clone(),
                other => panic!("expected string pk, got {other:?}"),
            };
            (pk, r.score)
        })
        .collect()
}

fn n_vec(ns: &[&str]) -> serde_json::Value {
    json!(ns.iter().map(|n| json!({"N": n})).collect::<Vec<_>>())
}

/// A table with one index per distance function over the suite's separation
/// fixture: `a` equals the query vector, `d` is a unit-norm mix, `b` is
/// orthogonal, `c` is opposite.
async fn create_distance_fixture(storage: &Storage, table: &str) {
    create_table(
        storage,
        json!({
            "TableName": table,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "cosine", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"}},
                {"IndexName": "euclidean", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "EUCLIDEAN",
                 "Projection": {"ProjectionType": "ALL"}},
                {"IndexName": "dot", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "DOT_PRODUCT",
                 "Projection": {"ProjectionType": "ALL"}}
            ]
        }),
    )
    .await;
    for (pk, v) in [
        ("a", ["1", "0", "0"]),
        ("b", ["0", "1", "0"]),
        ("c", ["-1", "0", "0"]),
        ("d", ["0.6", "0.8", "0"]),
    ] {
        put_item(
            storage,
            table,
            json!({
                "pk": {"S": pk},
                "label": {"S": format!("item-{pk}")},
                "embedding": {"L": n_vec(&v)}
            }),
        )
        .await;
    }
}

/// A table with a schemaless index and a HASH + INLINE_FILTER index, plus a
/// partitioned fixture: tenant t1 holds p1 (category c1), p2 (category c2),
/// and p4 (no category); tenant t2 holds p3 (category c1); p5 has no tenant.
async fn create_schema_fixture(storage: &Storage, table: &str) {
    create_table(
        storage,
        json!({
            "TableName": table,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "tenant", "AttributeType": "S"},
                {"AttributeName": "category", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "plain", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"}},
                {"IndexName": "schema", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"},
                 "SearchSchema": [
                     {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                     {"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"}
                 ]}
            ]
        }),
    )
    .await;
    for (pk, tenant, category) in [
        ("p1", Some("t1"), Some("c1")),
        ("p2", Some("t1"), Some("c2")),
        ("p3", Some("t2"), Some("c1")),
        ("p4", Some("t1"), None),
        ("p5", None, Some("c1")),
    ] {
        let mut item = json!({
            "pk": {"S": pk},
            "embedding": {"L": n_vec(&["1", "0", "0"])}
        });
        if let Some(t) = tenant {
            item["tenant"] = json!({"S": t});
        }
        if let Some(c) = category {
            item["category"] = json!({"S": c});
        }
        put_item(storage, table, item).await;
    }
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-6,
        "score {actual} not within 1e-6 of {expected}"
    );
}

// ---------------------------------------------------------------------------
// Scoring per distance function
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn cosine_self_match_scores_exactly_zero_and_ranks_lower_is_better() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecCos").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecCos", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();

    let got = scores(&resp);
    let order: Vec<&str> = got.iter().map(|(pk, _)| pk.as_str()).collect();
    assert_eq!(order, ["a", "d", "b", "c"]);
    assert_eq!(got[0].1, 0.0, "self-match must score exactly 0.0");
    assert_close(got[1].1, 0.4);
    assert_eq!(got[2].1, 1.0);
    assert_eq!(got[3].1, 2.0);
}

#[tokio::test(flavor = "current_thread")]
async fn euclidean_self_match_scores_exactly_zero_and_ranks_lower_is_better() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecEuc").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecEuc", "IndexName": "euclidean",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();

    let got = scores(&resp);
    let order: Vec<&str> = got.iter().map(|(pk, _)| pk.as_str()).collect();
    assert_eq!(order, ["a", "d", "b", "c"]);
    assert_eq!(got[0].1, 0.0, "self-match must score exactly 0.0");
    assert_close(got[1].1, 0.8f64.sqrt());
    assert_close(got[2].1, 2.0f64.sqrt());
    assert_eq!(got[3].1, 2.0);
}

#[tokio::test(flavor = "current_thread")]
async fn dot_product_ranks_higher_is_better_and_scores_can_be_negative() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecDot").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecDot", "IndexName": "dot",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();

    let got = scores(&resp);
    let order: Vec<&str> = got.iter().map(|(pk, _)| pk.as_str()).collect();
    assert_eq!(order, ["a", "d", "b", "c"]);
    assert_eq!(got[0].1, 1.0);
    assert_close(got[1].1, 0.6);
    assert_eq!(got[2].1, 0.0);
    assert_eq!(got[3].1, -1.0, "negative dot-product scores are legal");
}

// ---------------------------------------------------------------------------
// Zero vectors under COSINE (captured 2026-08-12: defined as exactly 1.0 with
// a zero-magnitude operand on either side, never NaN)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn cosine_zero_vectors_score_exactly_one_on_either_side() {
    let storage = Storage::memory().unwrap();
    create_table(
        &storage,
        json!({
            "TableName": "VecZero",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "cosine", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"}}
            ]
        }),
    )
    .await;
    put_item(
        &storage,
        "VecZero",
        json!({"pk": {"S": "zero"}, "embedding": {"L": n_vec(&["0", "0", "0"])}}),
    )
    .await;
    put_item(
        &storage,
        "VecZero",
        json!({"pk": {"S": "unit"}, "embedding": {"L": n_vec(&["1", "0", "0"])}}),
    )
    .await;

    // A zero query vector scores every item exactly 1.0, including the
    // stored zero vector.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecZero", "IndexName": "cosine",
            "SearchVector": n_vec(&["0", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();
    assert_eq!(resp.search_results.len(), 2);
    for result in &resp.search_results {
        assert_eq!(result.score, 1.0);
    }

    // A valid query scores the stored zero vector exactly 1.0 as well.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecZero", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();
    let got = scores(&resp);
    assert_eq!(got[0], ("unit".to_string(), 0.0));
    assert_eq!(got[1], ("zero".to_string(), 1.0));
}

// ---------------------------------------------------------------------------
// Tie-breaks: score then base-table primary key, deterministic across calls
// and across how the index was populated (a documented divergence: real
// DynamoDB's equal-score order is non-deterministic, captured 2026-08-12)
// ---------------------------------------------------------------------------

async fn put_identical_vectors_reversed(storage: &Storage, table: &str) {
    for pk in ["c", "b", "a"] {
        put_item(
            storage,
            table,
            json!({"pk": {"S": pk}, "embedding": {"L": n_vec(&["0.5", "0.5", "0"])}}),
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn equal_scores_order_by_primary_key_across_repeated_calls() {
    let storage = Storage::memory().unwrap();
    create_table(
        &storage,
        json!({
            "TableName": "VecTie",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "cosine", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"}}
            ]
        }),
    )
    .await;
    put_identical_vectors_reversed(&storage, "VecTie").await;

    let req = json!({
        "TableName": "VecTie", "IndexName": "cosine",
        "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
    });
    let first = scores(&search(&storage, req.clone()).await.unwrap());
    let order: Vec<&str> = first.iter().map(|(pk, _)| pk.as_str()).collect();
    assert_eq!(
        order,
        ["a", "b", "c"],
        "ties order by primary key regardless of insertion order"
    );
    assert!(first.windows(2).all(|w| w[0].1 == w[1].1));
    for _ in 0..5 {
        assert_eq!(scores(&search(&storage, req.clone()).await.unwrap()), first);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tie_order_is_identical_between_backfill_and_incremental_builds() {
    let storage = Storage::memory().unwrap();

    // Incremental: index exists first, items written through maintenance.
    create_table(
        &storage,
        json!({
            "TableName": "VecInc",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "vix", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"}}
            ]
        }),
    )
    .await;
    put_identical_vectors_reversed(&storage, "VecInc").await;

    // Backfill: items first, then the index arrives via UpdateTable.
    create_table(
        &storage,
        json!({
            "TableName": "VecBack",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    put_identical_vectors_reversed(&storage, "VecBack").await;
    let update: dynoxide::actions::update_table::UpdateTableRequest =
        serde_json::from_value(json!({
            "TableName": "VecBack",
            "VectorIndexUpdates": [{"Create": {
                "IndexName": "vix", "VectorAttribute": {"AttributeName": "embedding"},
                "Dimensions": 3, "DistanceFunction": "COSINE",
                "Projection": {"ProjectionType": "ALL"}
            }}]
        }))
        .unwrap();
    dynoxide::actions::update_table::execute(&storage, update)
        .await
        .unwrap();

    let query = |table: &str| {
        json!({
            "TableName": table, "IndexName": "vix",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        })
    };
    let incremental = scores(&search(&storage, query("VecInc")).await.unwrap());
    let backfilled = scores(&search(&storage, query("VecBack")).await.unwrap());
    assert_eq!(incremental, backfilled);
    let order: Vec<&str> = incremental.iter().map(|(pk, _)| pk.as_str()).collect();
    assert_eq!(order, ["a", "b", "c"]);
}

// ---------------------------------------------------------------------------
// TopK bounds
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn top_k_above_item_count_returns_everything_ranked() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecAll").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecAll", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 100
        }),
    )
    .await
    .unwrap();
    assert_eq!(resp.search_results.len(), 4);
    // SearchResults is the response's only data key: no pagination surface.
    let body = serde_json::to_string(&resp).unwrap();
    assert!(!body.contains("LastEvaluatedKey"));
    assert!(!body.contains("NextToken"));
}

#[tokio::test(flavor = "current_thread")]
async fn top_k_one_returns_the_best_match() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecOne").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecOne", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap();
    assert_eq!(scores(&resp), vec![("a".to_string(), 0.0)]);
}

#[test]
fn top_k_zero_rejects_at_the_request_model_layer_enveloped() {
    let err = request_model_error(serde_json::from_value(json!({
        "TableName": "T", "IndexName": "vix",
        "SearchVector": [{"N": "1"}], "TopK": 0
    })));
    assert_eq!(
        err,
        "1 validation error detected: Value '0' at 'topK' failed to satisfy constraint: \
         Member must have value greater than or equal to 1"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn top_k_101_rejects_bare_with_the_value_interpolated() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecBig").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecBig", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 101
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Provided TopK value '101' is out of valid range. The value must be between 1 and \
         100 inclusive"
    );
}

// ---------------------------------------------------------------------------
// SearchConditionExpression
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn hash_equality_scopes_the_search_to_one_partition() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecScope").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecScope", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tenant = :t",
            "ExpressionAttributeValues": {":t": {"S": "t1"}}
        }),
    )
    .await
    .unwrap();
    let mut pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    pks.sort();
    assert_eq!(pks, ["p1", "p2", "p4"]);

    let resp = search(
        &storage,
        json!({
            "TableName": "VecScope", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tenant = :t",
            "ExpressionAttributeValues": {":t": {"S": "t2"}}
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["p3"]);
}

#[tokio::test(flavor = "current_thread")]
async fn missing_condition_on_a_hash_schema_index_rejects() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecNoCond").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecNoCond", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "SearchConditionExpression must be provided when SearchSchema has a HASH key"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn non_equality_comparator_rejects_on_hash_and_inline_filter_elements() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecComp").await;

    let expected =
        "Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression";

    // On the HASH element.
    let err = search(
        &storage,
        json!({
            "TableName": "VecComp", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "tenant < :t",
            "ExpressionAttributeValues": {":t": {"S": "t9"}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), expected);

    // On the INLINE_FILTER element.
    let err = search(
        &storage,
        json!({
            "TableName": "VecComp", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "tenant = :t AND category < :c",
            "ExpressionAttributeValues": {":t": {"S": "t1"}, ":c": {"S": "c9"}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), expected);
}

#[tokio::test(flavor = "current_thread")]
async fn condition_attribute_outside_the_search_schema_rejects() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecOutside").await;

    // The message preserves AWS's own grammar exactly as captured.
    let err = search(
        &storage,
        json!({
            "TableName": "VecOutside", "IndexName": "plain",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "tenant = :t",
            "ExpressionAttributeValues": {":t": {"S": "t1"}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "SearchConditionExpression must not contain any attributes that is not in \
         SearchSchema. Invalid attribute: tenant"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn inline_filter_equality_narrows_and_absent_attributes_exclude() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecFilter").await;

    // Only p1 carries tenant t1 with category c1; p4 (t1, category absent)
    // is excluded by the filter.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecFilter", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tenant = :t AND category = :c",
            "ExpressionAttributeValues": {":t": {"S": "t1"}, ":c": {"S": "c1"}}
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["p1"]);
}

// ---------------------------------------------------------------------------
// Query-vector validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn dimension_mismatch_rejects_with_both_dimensions_interpolated() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecDims").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecDims", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Input search vector dimension 2 does not match vector index dimension 3"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_search_vector_elements_reject_with_the_captured_string() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecBad").await;

    let expected = "Search vector contains invalid values. All values in the search vector \
                    must be a 32-bit floating-point number attribute";

    // An out-of-f32-range element (captured 2026-08-12).
    let err = search(
        &storage,
        json!({
            "TableName": "VecBad", "IndexName": "cosine",
            "SearchVector": [{"N": "1E+39"}, {"N": "0"}, {"N": "0"}], "TopK": 1
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), expected);

    // An L-wrapped vector (captured 2026-08-11): the element check fires
    // before the dimension check even though the count also mismatches.
    let err = search(
        &storage,
        json!({
            "TableName": "VecBad", "IndexName": "cosine",
            "SearchVector": [{"L": n_vec(&["1", "0", "0"])}], "TopK": 1
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.to_string(), expected);
}

// ---------------------------------------------------------------------------
// Projection: the vector attribute is excluded by default and returned only
// when requested AND projected (captured)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn vector_attribute_is_absent_by_default_and_projected_on_request() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecProj").await;

    // Default: item keys only, no embedding.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecProj", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap();
    let item = &resp.search_results[0].item;
    let mut keys: Vec<&str> = item.keys().map(|k| k.as_str()).collect();
    keys.sort();
    assert_eq!(keys, ["label", "pk"]);

    // Requested and projected (ALL): the index hands back its own f32 copy,
    // serialised shortest-decimal, so "1" comes back as "1.0".
    let resp = search(
        &storage,
        json!({
            "TableName": "VecProj", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "ProjectionExpression": "pk, embedding"
        }),
    )
    .await
    .unwrap();
    let item = &resp.search_results[0].item;
    let embedding = match item.get("embedding") {
        Some(dynoxide::types::AttributeValue::L(l)) => l,
        other => panic!("expected projected embedding list, got {other:?}"),
    };
    let ns: Vec<&str> = embedding
        .iter()
        .map(|v| match v {
            dynoxide::types::AttributeValue::N(n) => n.as_str(),
            other => panic!("expected N element, got {other:?}"),
        })
        .collect();
    assert_eq!(ns, ["1.0", "0.0", "0.0"]);
}

#[tokio::test(flavor = "current_thread")]
async fn requested_vector_attribute_stays_absent_when_the_index_does_not_project_it() {
    let storage = Storage::memory().unwrap();
    create_table(
        &storage,
        json!({
            "TableName": "VecKeys",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "keysonly", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "KEYS_ONLY"}},
                {"IndexName": "with-vec", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["embedding"]}}
            ]
        }),
    )
    .await;
    put_item(
        &storage,
        "VecKeys",
        json!({"pk": {"S": "a"}, "embedding": {"L": n_vec(&["1", "0", "0"])}}),
    )
    .await;

    // KEYS_ONLY without the vector attribute: requested but not projected,
    // so it stays absent.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecKeys", "IndexName": "keysonly",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "ProjectionExpression": "pk, embedding"
        }),
    )
    .await
    .unwrap();
    let item = &resp.search_results[0].item;
    assert!(item.get("embedding").is_none());
    assert!(item.get("pk").is_some());

    // INCLUDE naming the vector attribute: requested and projected.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecKeys", "IndexName": "with-vec",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "ProjectionExpression": "pk, embedding"
        }),
    )
    .await
    .unwrap();
    assert!(resp.search_results[0].item.contains_key("embedding"));
}

// ---------------------------------------------------------------------------
// Nonexistent table and index
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn nonexistent_table_answers_resource_not_found() {
    let storage = Storage::memory().unwrap();

    // The conformance suite's data-plane probe depends on this shape: a real
    // ResourceNotFoundException, not an unsupported-operation fault.
    let err = search(
        &storage,
        json!({
            "TableName": "NoSuchTable", "IndexName": "no-such-index",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        dynoxide::DynoxideError::ResourceNotFoundException(_)
    ));
    assert_eq!(err.to_string(), "Requested resource not found");
}

#[tokio::test(flavor = "current_thread")]
async fn nonexistent_index_answers_the_captured_message() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecNoIx").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecNoIx", "IndexName": "absent",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "The table does not have the specified index: absent"
    );
}

// ---------------------------------------------------------------------------
// Expression attribute bookkeeping follows the Query family (uncaptured for
// this operation)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn expression_attribute_values_without_expressions_reject() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecEav").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecEav", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "ExpressionAttributeValues": {":t": {"S": "t1"}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "ExpressionAttributeValues can only be specified when using expressions: \
         SearchConditionExpression is null"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unused_expression_attribute_values_reject_after_the_search() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecUnused").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecUnused", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "tenant = :t",
            "ExpressionAttributeValues": {":t": {"S": "t1"}, ":spare": {"S": "x"}}
        }),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Value provided in ExpressionAttributeValues unused in expressions"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Scoring precision: f64 accumulation saturated to the f32 range (captured
// 2026-08-13, byte-identical in eu-west-2 and us-east-1)
// ---------------------------------------------------------------------------

/// A table with one two-dimensional index named `vix` using the given
/// distance function.
async fn create_two_dim_fixture(storage: &Storage, table: &str, distance: &str) {
    create_table(
        storage,
        json!({
            "TableName": table,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "vix", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 2, "DistanceFunction": distance,
                 "Projection": {"ProjectionType": "ALL"}}
            ]
        }),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dot_product_overflowing_products_cancel_to_exactly_zero() {
    let storage = Storage::memory().unwrap();
    create_two_dim_fixture(&storage, "VecDotOvf", "DOT_PRODUCT").await;
    put_item(
        &storage,
        "VecDotOvf",
        json!({"pk": {"S": "a"}, "embedding": {"L": n_vec(&["3.4e38", "-3.4e38"])}}),
    )
    .await;

    // Each product overflows f32 but the f64 accumulation cancels them
    // exactly; a naive f32 sum would report NaN (captured 2026-08-13).
    let resp = search(
        &storage,
        json!({
            "TableName": "VecDotOvf", "IndexName": "vix",
            "SearchVector": n_vec(&["3.4e38", "3.4e38"]), "TopK": 1
        }),
    )
    .await
    .unwrap();
    assert_eq!(scores(&resp), vec![("a".to_string(), 0.0)]);
}

#[tokio::test(flavor = "current_thread")]
async fn euclidean_overflowing_distance_saturates_to_f32_max() {
    let storage = Storage::memory().unwrap();
    create_two_dim_fixture(&storage, "VecEucOvf", "EUCLIDEAN").await;
    put_item(
        &storage,
        "VecEucOvf",
        json!({"pk": {"S": "a"}, "embedding": {"L": n_vec(&["-3.4e38", "0"])}}),
    )
    .await;

    // The difference overflows the f32 range, so the final score saturates
    // to exactly f32::MAX rather than reporting infinity (captured
    // 2026-08-13: exactly 3.4028234663852886e+38).
    let resp = search(
        &storage,
        json!({
            "TableName": "VecEucOvf", "IndexName": "vix",
            "SearchVector": n_vec(&["3.4e38", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap();
    let got = scores(&resp);
    assert_eq!(got, vec![("a".to_string(), f64::from(f32::MAX))]);
    assert_eq!(got[0].1, 3.4028234663852886e38);
}

#[tokio::test(flavor = "current_thread")]
async fn cosine_underflowing_self_match_scores_exactly_zero() {
    let storage = Storage::memory().unwrap();
    create_two_dim_fixture(&storage, "VecCosUdf", "COSINE").await;
    put_item(
        &storage,
        "VecCosUdf",
        json!({"pk": {"S": "tiny"}, "embedding": {"L": n_vec(&["1e-23", "0"])}}),
    )
    .await;

    // The f32 square of 1e-23 flushes to zero, but the f64 accumulation
    // keeps the magnitude nonzero, so the self-match scores 0.0 instead of
    // wrongly tripping the zero-magnitude branch's 1.0 (captured
    // 2026-08-13). The true-zero-vector case stays pinned at 1.0 by
    // cosine_zero_vectors_score_exactly_one_on_either_side.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecCosUdf", "IndexName": "vix",
            "SearchVector": n_vec(&["1e-23", "0"]), "TopK": 1
        }),
    )
    .await
    .unwrap();
    assert_eq!(scores(&resp), vec![("tiny".to_string(), 0.0)]);
}

// ---------------------------------------------------------------------------
// HASH operand validation (captured 2026-08-13, byte-identical in eu-west-2
// and us-east-1) and the remaining uncaptured match-nothing shapes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn empty_string_hash_operand_rejects_with_the_key_suffix() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecEmptyKey").await;

    // A "Key:" suffix, distinct from the write path's IndexName/IndexKey
    // form (captured 2026-08-13).
    let err = search(
        &storage,
        json!({
            "TableName": "VecEmptyKey", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "tenant = :t",
            "ExpressionAttributeValues": {":t": {"S": ""}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "One or more parameter values are not valid. The AttributeValue for a key attribute \
         cannot contain an empty string value. Key: tenant"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn type_mismatched_hash_operand_rejects_with_both_types() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecTypeMis").await;

    // Captured for an N operand against the declared S (2026-08-13); the
    // BOOL, L, and M letters are uncaptured within the captured format.
    for (operand, letter) in [
        (json!({"N": "1"}), "N"),
        (json!({"BOOL": true}), "BOOL"),
        (json!({"L": [{"S": "t1"}]}), "L"),
        (json!({"M": {"v": {"S": "t1"}}}), "M"),
    ] {
        let err = search(
            &storage,
            json!({
                "TableName": "VecTypeMis", "IndexName": "schema",
                "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
                "SearchConditionExpression": "tenant = :t",
                "ExpressionAttributeValues": {":t": operand}
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "Type of 'tenant' attribute in SearchConditionExpression ({letter}) does not \
                 match type in search schema (S)"
            )
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn contradictory_hash_equalities_match_nothing() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecContra").await;

    // Two same-type equalities naming different values can never both equal
    // one stored key string, so the search matches nothing rather than
    // erroring (uncaptured).
    let resp = search(
        &storage,
        json!({
            "TableName": "VecContra", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tenant = :a AND tenant = :b",
            "ExpressionAttributeValues": {":a": {"S": "t1"}, ":b": {"S": "t2"}}
        }),
    )
    .await
    .unwrap();
    assert!(resp.search_results.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn nested_path_conjunct_matches_nothing() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecNested").await;

    // A nested path under a schema attribute can never equal a stored scalar
    // schema value, so the conjunct matches nothing (uncaptured; the
    // top-level name still counts for schema membership).
    let resp = search(
        &storage,
        json!({
            "TableName": "VecNested", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tenant = :t AND category.child = :c",
            "ExpressionAttributeValues": {":t": {"S": "t1"}, ":c": {"S": "c1"}}
        }),
    )
    .await
    .unwrap();
    assert!(resp.search_results.is_empty());
}

// ---------------------------------------------------------------------------
// Expression reference errors carry the operation's prefix
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn undefined_expression_attribute_value_rejects_with_the_prefix() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecNoVal").await;

    let err = search(
        &storage,
        json!({
            "TableName": "VecNoVal", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "tenant = :t AND category = :c",
            "ExpressionAttributeValues": {":t": {"S": "t1"}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid SearchConditionExpression: An expression attribute value used in expression \
         is not defined; attribute value: :c"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reserved_word_in_condition_rejects_with_the_captured_prefix() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecReserved").await;

    // Captured 2026-08-13: the reserved-word rejection carries the
    // operation's prefix through the shared condition machinery.
    let err = search(
        &storage,
        json!({
            "TableName": "VecReserved", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
            "SearchConditionExpression": "bucket = :t",
            "ExpressionAttributeValues": {":t": {"S": "t1"}}
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid SearchConditionExpression: Attribute name is a reserved keyword; \
         reserved keyword: bucket"
    );
}

// ---------------------------------------------------------------------------
// Numeric spelling normalisation (HASH scope captured 2026-08-13; the
// INLINE_FILTER side goes through the condition machinery's numeric equality)
// ---------------------------------------------------------------------------

/// A table whose index keys its SearchSchema HASH on an N-typed attribute:
/// g1 sits in partition 1, g2 in partition 2.
async fn create_numeric_hash_fixture(storage: &Storage, table: &str) {
    create_table(
        storage,
        json!({
            "TableName": table,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "grp", "AttributeType": "N"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "schema", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"},
                 "SearchSchema": [
                     {"AttributeName": "grp", "SearchSchemaElementType": "HASH"}
                 ]}
            ]
        }),
    )
    .await;
    for (pk, grp) in [("g1", "1"), ("g2", "2")] {
        put_item(
            storage,
            table,
            json!({
                "pk": {"S": pk},
                "grp": {"N": grp},
                "embedding": {"L": n_vec(&["1", "0", "0"])}
            }),
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn numeric_hash_operand_spelling_normalises() {
    let storage = Storage::memory().unwrap();
    create_numeric_hash_fixture(&storage, "VecNumHash").await;

    // {N: "1.0"} matches stored {N: "1"}: the key-string encoding
    // normalises numeric spellings on both sides (captured 2026-08-13).
    let resp = search(
        &storage,
        json!({
            "TableName": "VecNumHash", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "grp = :g",
            "ExpressionAttributeValues": {":g": {"N": "1.0"}}
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["g1"]);
}

/// A table whose index declares an INLINE_FILTER-only SearchSchema on an
/// N-typed attribute, with no HASH element: a sits at tier 1, b at tier 2,
/// c carries no tier.
async fn create_inline_filter_fixture(storage: &Storage, table: &str) {
    create_table(
        storage,
        json!({
            "TableName": table,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "tier", "AttributeType": "N"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "filtered", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"},
                 "SearchSchema": [
                     {"AttributeName": "tier", "SearchSchemaElementType": "INLINE_FILTER"}
                 ]}
            ]
        }),
    )
    .await;
    for (pk, tier) in [("a", Some("1")), ("b", Some("2")), ("c", None)] {
        let mut item = json!({
            "pk": {"S": pk},
            "embedding": {"L": n_vec(&["1", "0", "0"])}
        });
        if let Some(t) = tier {
            item["tier"] = json!({"N": t});
        }
        put_item(storage, table, item).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inline_filter_numeric_equality_normalises_spelling() {
    let storage = Storage::memory().unwrap();
    create_inline_filter_fixture(&storage, "VecIfNum").await;

    // {N: "1.0"} matches stored {N: "1"} through the condition machinery's
    // numeric equality.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecIfNum", "IndexName": "filtered",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tier = :r",
            "ExpressionAttributeValues": {":r": {"N": "1.0"}}
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["a"]);
}

#[tokio::test(flavor = "current_thread")]
async fn hashless_schema_accepts_a_condition_without_requiring_one() {
    let storage = Storage::memory().unwrap();
    create_inline_filter_fixture(&storage, "VecIfOnly").await;

    // No HASH element, so no mandatory-condition rejection: a search without
    // any condition returns everything, and a filter condition narrows.
    let resp = search(
        &storage,
        json!({
            "TableName": "VecIfOnly", "IndexName": "filtered",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();
    assert_eq!(resp.search_results.len(), 3);

    let resp = search(
        &storage,
        json!({
            "TableName": "VecIfOnly", "IndexName": "filtered",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tier = :r",
            "ExpressionAttributeValues": {":r": {"N": "2"}}
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["b"]);
}

// ---------------------------------------------------------------------------
// Condition shapes: name aliasing, multi-filter AND chains, parentheses
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn expression_attribute_names_alias_resolves_to_the_hash_attribute() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecAlias").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecAlias", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "#t = :v",
            "ExpressionAttributeNames": {"#t": "tenant"},
            "ExpressionAttributeValues": {":v": {"S": "t1"}}
        }),
    )
    .await
    .unwrap();
    let mut pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    pks.sort();
    assert_eq!(pks, ["p1", "p2", "p4"]);
}

/// A table whose index chains a HASH element with two INLINE_FILTER elements.
async fn create_multi_filter_fixture(storage: &Storage, table: &str) {
    create_table(
        storage,
        json!({
            "TableName": table,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "tenant", "AttributeType": "S"},
                {"AttributeName": "category", "AttributeType": "S"},
                {"AttributeName": "tier", "AttributeType": "N"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "schema", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 3, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"},
                 "SearchSchema": [
                     {"AttributeName": "tenant", "SearchSchemaElementType": "HASH"},
                     {"AttributeName": "category", "SearchSchemaElementType": "INLINE_FILTER"},
                     {"AttributeName": "tier", "SearchSchemaElementType": "INLINE_FILTER"}
                 ]}
            ]
        }),
    )
    .await;
    for (pk, tenant, category, tier) in [
        ("m1", "t1", "c1", "1"),
        ("m2", "t1", "c1", "2"),
        ("m3", "t1", "c2", "1"),
        ("m4", "t2", "c1", "1"),
    ] {
        put_item(
            storage,
            table,
            json!({
                "pk": {"S": pk},
                "tenant": {"S": tenant},
                "category": {"S": category},
                "tier": {"N": tier},
                "embedding": {"L": n_vec(&["1", "0", "0"])}
            }),
        )
        .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn multi_inline_filter_and_chain_narrows_to_the_intersection() {
    let storage = Storage::memory().unwrap();
    create_multi_filter_fixture(&storage, "VecMulti").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecMulti", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "tenant = :t AND category = :c AND tier = :r",
            "ExpressionAttributeValues": {
                ":t": {"S": "t1"}, ":c": {"S": "c1"}, ":r": {"N": "1"}
            }
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["m1"]);
}

#[tokio::test(flavor = "current_thread")]
async fn parenthesised_and_shape_is_accepted() {
    let storage = Storage::memory().unwrap();
    create_schema_fixture(&storage, "VecParen").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecParen", "IndexName": "schema",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10,
            "SearchConditionExpression": "(tenant = :t AND category = :c)",
            "ExpressionAttributeValues": {":t": {"S": "t1"}, ":c": {"S": "c1"}}
        }),
    )
    .await
    .unwrap();
    let pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    assert_eq!(pks, ["p1"]);
}

// ---------------------------------------------------------------------------
// Request-model constraints (captured 2026-08-13, byte-identical in
// eu-west-2 and us-east-1: the no-echo forms)
// ---------------------------------------------------------------------------

#[test]
fn short_index_name_rejects_without_a_value_echo() {
    // "Value at", not "Value '<x>' at" (captured 2026-08-13).
    let err = request_model_error(serde_json::from_value(json!({
        "TableName": "T", "IndexName": "ab",
        "SearchVector": [{"N": "1"}], "TopK": 1
    })));
    assert_eq!(
        err,
        "1 validation error detected: Value at 'IndexName' failed to satisfy constraint: \
         Member must have length greater than or equal to 3"
    );
}

#[test]
fn overlong_index_name_rejects_with_the_upper_bound() {
    // The 255 upper bound mirrors the CreateTable family; its wording for
    // this operation is uncaptured beyond the captured no-echo form.
    let err = request_model_error(serde_json::from_value(json!({
        "TableName": "T", "IndexName": "x".repeat(256),
        "SearchVector": [{"N": "1"}], "TopK": 1
    })));
    assert_eq!(
        err,
        "1 validation error detected: Value at 'IndexName' failed to satisfy constraint: \
         Member must have length less than or equal to 255"
    );
}

#[test]
fn empty_search_vector_rejects_without_a_value_echo() {
    let err = request_model_error(serde_json::from_value(json!({
        "TableName": "T", "IndexName": "vix",
        "SearchVector": [], "TopK": 1
    })));
    assert_eq!(
        err,
        "1 validation error detected: Value at 'SearchVector' failed to satisfy constraint: \
         Member must have length greater than or equal to 1"
    );
}

// ---------------------------------------------------------------------------
// TopK shapes that never reach the request model: the i32 field rejects them
// inside serde (captured 2026-08-13 over the wire, where serde_json's
// position suffix is naturally present; `from_value` has no position, so
// these pin the captured message minus the tail)
// ---------------------------------------------------------------------------

#[test]
fn top_k_fractional_rejects_inside_serde_with_the_captured_message_shape() {
    let err = serde_json::from_value::<SearchVectorsRequest>(json!({
        "TableName": "T", "IndexName": "vix",
        "SearchVector": [{"N": "1"}], "TopK": 3.5
    }))
    .unwrap_err()
    .to_string();
    assert_eq!(err, "invalid type: floating point `3.5`, expected i32");
}

#[test]
fn top_k_out_of_range_floats_reject_without_echoing_saturated_sentinels() {
    for (v, spelled) in [(1e300, "1e+300"), (-1e300, "-1e+300")] {
        let err = serde_json::from_value::<SearchVectorsRequest>(json!({
            "TableName": "T", "IndexName": "vix",
            "SearchVector": [{"N": "1"}], "TopK": v
        }))
        .unwrap_err()
        .to_string();
        assert_eq!(
            err,
            format!("invalid type: floating point `{spelled}`, expected i32"),
            "no saturated i64/i32 sentinel may leak into the message"
        );
    }
}

// ---------------------------------------------------------------------------
// Response surface and index maintenance end to end
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn empty_table_search_returns_an_empty_search_results_array() {
    let storage = Storage::memory().unwrap();
    create_two_dim_fixture(&storage, "VecNone", "COSINE").await;

    let resp = search(
        &storage,
        json!({
            "TableName": "VecNone", "IndexName": "vix",
            "SearchVector": n_vec(&["1", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_string(&resp).unwrap(),
        r#"{"SearchResults":[]}"#
    );
}

#[tokio::test(flavor = "current_thread")]
async fn consumed_capacity_is_absent_under_indexes_and_total() {
    // Capacity reporting for SearchVectors is not wired up yet; the
    // dedicated vector capacity work replaces this pin when it lands.
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecCap").await;

    for rcc in ["INDEXES", "TOTAL"] {
        let resp = search(
            &storage,
            json!({
                "TableName": "VecCap", "IndexName": "cosine",
                "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 1,
                "ReturnConsumedCapacity": rcc
            }),
        )
        .await
        .unwrap();
        let body = serde_json::to_string(&resp).unwrap();
        assert!(
            !body.contains("ConsumedCapacity"),
            "unexpected capacity under {rcc}: {body}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn remove_update_deindexes_the_item_end_to_end() {
    let storage = Storage::memory().unwrap();
    create_distance_fixture(&storage, "VecRemove").await;

    let update: dynoxide::actions::update_item::UpdateItemRequest = serde_json::from_value(json!({
        "TableName": "VecRemove",
        "Key": {"pk": {"S": "a"}},
        "UpdateExpression": "REMOVE embedding"
    }))
    .unwrap();
    dynoxide::actions::update_item::execute(&storage, update)
        .await
        .unwrap();

    let resp = search(
        &storage,
        json!({
            "TableName": "VecRemove", "IndexName": "cosine",
            "SearchVector": n_vec(&["1", "0", "0"]), "TopK": 10
        }),
    )
    .await
    .unwrap();
    let mut pks: Vec<String> = scores(&resp).into_iter().map(|(pk, _)| pk).collect();
    pks.sort();
    assert_eq!(pks, ["b", "c", "d"], "the de-indexed item must not score");
}

#[tokio::test(flavor = "current_thread")]
async fn four_thousand_ninety_six_dimension_vectors_search() {
    let storage = Storage::memory().unwrap();
    create_table(
        &storage,
        json!({
            "TableName": "VecWide",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {"IndexName": "vix", "VectorAttribute": {"AttributeName": "embedding"},
                 "Dimensions": 4096, "DistanceFunction": "COSINE",
                 "Projection": {"ProjectionType": "ALL"}}
            ]
        }),
    )
    .await;

    // Unit basis vectors: e0 self-matches the query, e1 is orthogonal.
    let basis = |one_at: usize| -> serde_json::Value {
        json!(
            (0..4096)
                .map(|i| json!({"N": if i == one_at { "1" } else { "0" }}))
                .collect::<Vec<_>>()
        )
    };
    for (pk, at) in [("a", 0), ("b", 1)] {
        put_item(
            &storage,
            "VecWide",
            json!({"pk": {"S": pk}, "embedding": {"L": basis(at)}}),
        )
        .await;
    }

    let resp = search(
        &storage,
        json!({
            "TableName": "VecWide", "IndexName": "vix",
            "SearchVector": basis(0), "TopK": 10
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        scores(&resp),
        vec![("a".to_string(), 0.0), ("b".to_string(), 1.0)]
    );
}
