//! PartiQL statement parsing and execution for DynamoDB.
//!
//! Supports SELECT, INSERT, UPDATE, DELETE statements mapped to
//! underlying DynamoDB operations.

pub mod executor;
pub mod parser;
