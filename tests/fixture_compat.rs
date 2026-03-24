//! Tests that verify database compatibility between Option A (SQLCipher always linked)
//! and Option B (plain SQLite default, SQLCipher behind feature flag).
//!
//! These tests run under default features (plain SQLite) and read fixtures
//! that were created by the SQLCipher-linked build.

use dynoxide::Database;

const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

#[test]
fn option_a_unencrypted_db_readable_by_plain_sqlite() {
    let path = format!("{FIXTURE_DIR}/option-a-unencrypted.db");
    let db = Database::new(&path).expect("Should open Option A unencrypted DB with plain SQLite");

    // Verify the data written by the SQLCipher-linked build is readable
    let key = dynoxide::item! { "pk" => "user#1" };
    let resp = db
        .get_item(dynoxide::actions::get_item::GetItemRequest {
            table_name: "Users".to_string(),
            key,
            ..Default::default()
        })
        .expect("GetItem should succeed");

    let item = resp.item.expect("Item should exist");
    assert_eq!(
        item.get("name").unwrap(),
        &dynoxide::AttributeValue::S("Alice".to_string()),
    );
}

#[test]
fn option_a_encrypted_db_gives_clear_error() {
    let path = format!("{FIXTURE_DIR}/option-a-encrypted.db");
    let result = Database::new(&path);

    match result {
        Ok(_) => panic!("Opening encrypted DB without key should fail"),
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("encrypted") || msg.contains("not a valid"),
                "Error should mention encryption, got: {msg}"
            );
        }
    }
}
