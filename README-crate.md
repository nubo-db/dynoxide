# dynoxide-rs

[![crates.io](https://img.shields.io/crates/v/dynoxide-rs.svg)](https://crates.io/crates/dynoxide-rs) [![docs.rs](https://img.shields.io/docsrs/dynoxide-rs)](https://docs.rs/dynoxide-rs) [![conformance](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/paritysuite/dynamodb-conformance/main/results/dynoxide.badge.json)](https://paritysuite.org) [![license](https://img.shields.io/crates/l/dynoxide-rs.svg)](https://github.com/nubo-db/dynoxide#license)

An embeddable DynamoDB emulator backed by SQLite. Starts in under a
millisecond, needs no JVM and no Docker, and is verified against real AWS by a
public conformance suite.

This is the Rust library. Dynoxide also ships as a CLI, an HTTP server, an MCP
server for coding agents, a container image and a WebAssembly build for the
browser. See [the repository](https://github.com/nubo-db/dynoxide) for those.

## Using it

```toml
[dependencies]
dynoxide-rs = "2.0"
```

```rust
use dynoxide::Database;
use serde_json::json;

// In-memory, which is what most tests want. Persistent and encrypted
// databases are `Database::new` and `Database::new_encrypted`.
let db = Database::memory().unwrap();

let req = serde_json::from_value(json!({
    "TableName": "Users",
    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
})).unwrap();
db.create_table(req).unwrap();

let req = serde_json::from_value(json!({
    "TableName": "Users",
    "Item": {"pk": {"S": "user#1"}, "name": {"S": "Alice"}}
})).unwrap();
db.put_item(req).unwrap();
```

Requests and responses are DynamoDB-shaped, so a test written against Dynoxide
reads like one written against the real thing.

The default features pull in the CLI, HTTP and MCP servers. For the library
alone:

```toml
dynoxide-rs = { version = "2.0", default-features = false, features = ["native-sqlite"] }
```

[Library guide](https://github.com/nubo-db/dynoxide/blob/main/docs/library.md)
and [API documentation](https://docs.rs/dynoxide-rs).

## Two version numbers

The number on this page is the **crate** version. It moves when the Rust API
changes.

Everything you install rather than depend on carries a separate **product**
version: the `dynoxide` binary, the npm packages, the container images, the
browser engine and the GitHub Action. That number is what `dynoxide --version`
prints, and it is the one to quote in a bug report.

So `cargo install dynoxide-rs --version 2.0.0` gives you a binary that reports
a 1.x product version, and that is not a mistake. The two streams were split
so that a Rust API break does not force a major release on people who never
touch the Rust API.

[The versioning policy](https://github.com/nubo-db/dynoxide/blob/main/docs/versioning.md)
explains what each number promises.

## Conformance

Dynoxide is checked against real DynamoDB by a
[public conformance suite](https://paritysuite.org). The badge above is that
suite's current result. It is not a claim that conformance is finished, and
[the compatibility summary](https://github.com/nubo-db/dynoxide/blob/main/docs/compatibility-summary.md)
records what is and is not covered.

## Licence

MIT or Apache-2.0, at your option.
