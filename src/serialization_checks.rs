//! Pre-deserialisation JSON type checks for SerializationException parity.
//!
//! Request structs use `Option<serde_json::Value>` for several fields, so
//! serde accepts any JSON type there. These checks inspect the raw JSON
//! first and return the SerializationException DynamoDB would produce for
//! a type mismatch, before serde gets involved.

use std::borrow::Cow;

mod positional;

/// The refusal for a JSON array where DynamoDB expects a scalar, a map or a
/// structure, and for a string or a number given as a BatchGetItem key or as
/// a table's BatchWriteItem requests: a `SerializationException` with no
/// message (captured eu-west-2 and us-east-1, 11 and 12 November 2026).
fn array_not_expected() -> crate::DynoxideError {
    crate::DynoxideError::SerializationException(String::new())
}

/// Pre-check JSON field types that are deserialized as `serde_json::Value`.
///
/// DynamoDB returns SerializationException for type mismatches on fields like
/// AttributeDefinitions, KeySchema, etc. Because our raw request structs use
/// `Option<serde_json::Value>` for these fields, serde accepts any JSON type.
/// This function inspects the raw JSON and returns the appropriate
/// SerializationException before serde gets involved.
///
/// Returns the body the request is read from: `body` itself, or, when it
/// holds a form DynamoDB reads by position, `body` with that form rewritten to
/// the object form it stands for (see [`positional`]).
pub(crate) fn pre_check_serialization_types<'a>(
    operation: &str,
    body: &'a str,
) -> crate::Result<Cow<'a, str>> {
    pre_check_and_read_layer(operation, body).map(|(body, _)| body)
}

/// [`pre_check_serialization_types`], then the validation layer the request's
/// values are read on: the operation's own, or the older layer for a request
/// that holds a boolean member written as a string or a value with more than
/// one member, whatever the operation (see
/// [`crate::types::reads_on_older_layer`]). The body is parsed once for both,
/// or twice when a positional form is rewritten. The caller reads the request
/// from the body returned beside the layer.
pub(crate) fn pre_check_and_read_layer<'a>(
    operation: &str,
    body: &'a str,
) -> crate::Result<(Cow<'a, str>, crate::validation::ValueLayer)> {
    let read = |body: &str| {
        crate::serde_errors::body_value(body)
            .map_err(|e| crate::DynoxideError::SerializationException(e.to_string()))
    };
    let json = read(body)?;
    let (body, json) = match positional::rewrite(operation, body) {
        Some(rewritten) => {
            let json = read(&rewritten)?;
            (Cow::Owned(rewritten), json)
        }
        None => (Cow::Borrowed(body), json),
    };
    check_types(operation, &json)?;
    let layer = if crate::types::reads_on_older_layer(&json) {
        crate::validation::ValueLayer::Older
    } else {
        crate::validation::ValueLayer::for_operation(operation)
    };
    Ok((body, layer))
}

fn check_types(operation: &str, json: &serde_json::Value) -> crate::Result<()> {
    // SearchVectors is served by its own endpoint, whose strict deserialiser
    // answers every JSON type mismatch in its own words (see
    // `actions::search_vectors`), a body that isn't an object included, so
    // none of the checks here apply to it.
    if operation == "SearchVectors" {
        return Ok(());
    }

    // The body itself must be an object. DynamoDB refuses an array with no
    // message, a string, number or boolean as a value of the wrong type, and
    // `null` as holding no request (captured eu-west-2 and us-east-1,
    // 11 November 2026). A BatchGetItem or BatchWriteItem body read by
    // position has been rewritten to an object by now.
    let obj = match json {
        serde_json::Value::Object(o) => o,
        serde_json::Value::Array(_) => return Err(array_not_expected()),
        serde_json::Value::Null => {
            return Err(crate::DynoxideError::SerializationException(
                "Request payload does not contain the input model".to_string(),
            ));
        }
        _ => {
            return Err(crate::DynoxideError::SerializationException(
                "Unexpected value type in payload".to_string(),
            ));
        }
    };

    match operation {
        "CreateTable" => {
            check_field_is_list(obj, "AttributeDefinitions")?;
            check_field_is_list(obj, "KeySchema")?;
            check_field_is_list(obj, "LocalSecondaryIndexes")?;
            check_field_is_list(obj, "GlobalSecondaryIndexes")?;
            check_field_is_list(obj, "VectorIndexes")?;
            check_list_elements_are_structs(obj, "AttributeDefinitions")?;
            check_list_elements_are_structs(obj, "KeySchema")?;
            check_list_elements_are_structs(obj, "LocalSecondaryIndexes")?;
            check_list_elements_are_structs(obj, "GlobalSecondaryIndexes")?;
            check_list_elements_are_structs(obj, "VectorIndexes")?;

            // Check struct fields and their inner scalar types
            check_field_is_struct(obj, "ProvisionedThroughput")?;
            check_nested_pt_fields(obj)?;
            refuse_arrays(operation, obj)?;

            // Check nested fields inside KeySchema elements
            check_nested_list_structs(obj, "KeySchema")?;
            // Check nested fields inside AttributeDefinitions elements
            check_nested_list_structs(obj, "AttributeDefinitions")?;

            // Check nested list fields inside LocalSecondaryIndexes
            if let Some(serde_json::Value::Array(arr)) = obj.get("LocalSecondaryIndexes") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Projection")?;
                        check_field_is_list(inner, "KeySchema")?;
                        check_list_elements_are_structs(inner, "KeySchema")?;
                        check_field_is_string(inner, "IndexName")?;
                        check_nested_list_structs(inner, "KeySchema")?;
                        check_nested_projection_fields(inner)?;
                        if let Some(proj) = inner.get("Projection").and_then(|p| p.as_object()) {
                            check_field_is_list(proj, "NonKeyAttributes")?;
                            check_nested_list_strings(proj, "NonKeyAttributes")?;
                        }
                    }
                }
            }

            // Check nested list fields inside GlobalSecondaryIndexes
            if let Some(serde_json::Value::Array(arr)) = obj.get("GlobalSecondaryIndexes") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Projection")?;
                        check_field_is_struct(inner, "ProvisionedThroughput")?;
                        check_field_is_list(inner, "KeySchema")?;
                        check_list_elements_are_structs(inner, "KeySchema")?;
                        check_field_is_string(inner, "IndexName")?;
                        check_nested_list_structs(inner, "KeySchema")?;
                        check_nested_projection_fields(inner)?;
                        check_nested_pt_fields(inner)?;
                        if let Some(proj) = inner.get("Projection").and_then(|p| p.as_object()) {
                            check_field_is_list(proj, "NonKeyAttributes")?;
                            check_nested_list_strings(proj, "NonKeyAttributes")?;
                        }
                    }
                }
            }

            // Check nested fields inside VectorIndexes
            if let Some(serde_json::Value::Array(arr)) = obj.get("VectorIndexes") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "VectorAttribute")?;
                        check_field_is_struct(inner, "Projection")?;
                        check_field_is_list(inner, "SearchSchema")?;
                        check_list_elements_are_structs(inner, "SearchSchema")?;
                        check_field_is_string(inner, "IndexName")?;
                        check_field_is_string(inner, "DistanceFunction")?;
                        check_field_is_int(inner, "Dimensions")?;
                        check_nested_projection_fields(inner)?;
                        if let Some(va) = inner.get("VectorAttribute").and_then(|v| v.as_object()) {
                            check_field_is_string(va, "AttributeName")?;
                        }
                        if let Some(serde_json::Value::Array(schema)) = inner.get("SearchSchema") {
                            for elem in schema {
                                if let Some(eo) = elem.as_object() {
                                    check_field_is_string(eo, "AttributeName")?;
                                    check_field_is_string(eo, "SearchSchemaElementType")?;
                                }
                            }
                        }
                        if let Some(proj) = inner.get("Projection").and_then(|p| p.as_object()) {
                            check_field_is_list(proj, "NonKeyAttributes")?;
                            check_nested_list_strings(proj, "NonKeyAttributes")?;
                        }
                    }
                }
            }
        }
        "UpdateTable" => {
            check_field_is_list(obj, "GlobalSecondaryIndexUpdates")?;
            check_list_elements_are_structs(obj, "GlobalSecondaryIndexUpdates")?;
            check_field_is_struct(obj, "ProvisionedThroughput")?;
            check_nested_pt_fields(obj)?;
            refuse_arrays(operation, obj)?;
            // Check inside GlobalSecondaryIndexUpdates
            if let Some(serde_json::Value::Array(arr)) = obj.get("GlobalSecondaryIndexUpdates") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Create")?;
                        check_field_is_struct(inner, "Update")?;
                        check_field_is_struct(inner, "Delete")?;
                        if let Some(create) = inner.get("Create").and_then(|v| v.as_object()) {
                            check_field_is_struct(create, "Projection")?;
                            check_field_is_struct(create, "ProvisionedThroughput")?;
                            check_field_is_list(create, "KeySchema")?;
                            check_list_elements_are_structs(create, "KeySchema")?;
                            check_nested_list_structs(create, "KeySchema")?;
                            check_nested_projection_fields(create)?;
                            check_nested_pt_fields(create)?;
                        }
                        if let Some(update) = inner.get("Update").and_then(|v| v.as_object()) {
                            check_field_is_struct(update, "ProvisionedThroughput")?;
                            check_nested_pt_fields(update)?;
                        }
                    }
                }
            }
            // Check inside VectorIndexUpdates; the Create action carries the
            // same shape as a CreateTable VectorIndexes entry.
            check_field_is_list(obj, "VectorIndexUpdates")?;
            check_list_elements_are_structs(obj, "VectorIndexUpdates")?;
            if let Some(serde_json::Value::Array(arr)) = obj.get("VectorIndexUpdates") {
                for item in arr {
                    if let Some(inner) = item.as_object() {
                        check_field_is_struct(inner, "Create")?;
                        check_field_is_struct(inner, "Delete")?;
                        if let Some(create) = inner.get("Create").and_then(|v| v.as_object()) {
                            check_field_is_struct(create, "VectorAttribute")?;
                            check_field_is_struct(create, "Projection")?;
                            check_field_is_list(create, "SearchSchema")?;
                            check_list_elements_are_structs(create, "SearchSchema")?;
                            check_field_is_string(create, "IndexName")?;
                            check_field_is_string(create, "DistanceFunction")?;
                            check_field_is_int(create, "Dimensions")?;
                            check_nested_projection_fields(create)?;
                            if let Some(va) =
                                create.get("VectorAttribute").and_then(|v| v.as_object())
                            {
                                check_field_is_string(va, "AttributeName")?;
                            }
                            if let Some(serde_json::Value::Array(schema)) =
                                create.get("SearchSchema")
                            {
                                for elem in schema {
                                    if let Some(eo) = elem.as_object() {
                                        check_field_is_string(eo, "AttributeName")?;
                                        check_field_is_string(eo, "SearchSchemaElementType")?;
                                    }
                                }
                            }
                            if let Some(proj) = create.get("Projection").and_then(|p| p.as_object())
                            {
                                check_field_is_list(proj, "NonKeyAttributes")?;
                                check_nested_list_strings(proj, "NonKeyAttributes")?;
                            }
                        }
                        if let Some(delete) = inner.get("Delete").and_then(|v| v.as_object()) {
                            check_field_is_string(delete, "IndexName")?;
                        }
                    }
                }
            }
        }
        "PutItem" | "DeleteItem" | "UpdateItem" => {
            check_field_is_map(obj, "AttributeUpdates")?;
            check_map_values_are_structs(obj, "AttributeUpdates")?;
        }
        "Query" => {
            check_field_is_map(obj, "KeyConditions")?;
            check_field_is_map(obj, "QueryFilter")?;
            check_map_values_are_structs(obj, "QueryFilter")?;
            check_map_values_are_structs(obj, "KeyConditions")?;
            check_filter_inner_fields(obj, "QueryFilter")?;
            check_filter_inner_fields(obj, "KeyConditions")?;
            check_filter_attribute_value_lists(obj, "QueryFilter")?;
            check_field_is_map(obj, "ExclusiveStartKey")?;
        }
        "Scan" => {
            check_field_is_map(obj, "ScanFilter")?;
            check_map_values_are_structs(obj, "ScanFilter")?;
            check_filter_inner_fields(obj, "ScanFilter")?;
            check_filter_attribute_value_lists(obj, "ScanFilter")?;
            check_field_is_map(obj, "ExclusiveStartKey")?;
        }
        "BatchGetItem" => {
            check_field_is_map(obj, "RequestItems")?;
            check_map_values_are_structs(obj, "RequestItems")?;
            // Check nested fields inside RequestItems
            if let Some(serde_json::Value::Object(ri)) = obj.get("RequestItems") {
                for (_table, val) in ri {
                    if let Some(inner) = val.as_object() {
                        check_field_is_map(inner, "ExpressionAttributeNames")?;
                        // Check Keys array elements are maps, and their values are AV structs
                        if let Some(serde_json::Value::Array(keys)) = inner.get("Keys") {
                            for key in keys {
                                // A key given as anything but a map is refused
                                // with no message, a string and a number as an
                                // array is (captured eu-west-2 and us-east-1,
                                // 11 and 12 November 2026). A null key is
                                // refused once the request is read.
                                if !key.is_object() && !key.is_null() {
                                    return Err(array_not_expected());
                                }
                                if let Some(key_map) = key.as_object() {
                                    for (_k, v) in key_map {
                                        if v.is_array() {
                                            return Err(array_not_expected());
                                        }
                                        if !v.is_object() && !v.is_null() {
                                            return Err(
                                                crate::DynoxideError::SerializationException(
                                                    "Unexpected value type in payload".to_string(),
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "BatchWriteItem" => {
            check_field_is_map(obj, "RequestItems")?;
            // Check nested fields inside RequestItems
            if let Some(serde_json::Value::Object(ri)) = obj.get("RequestItems") {
                for (_table, val) in ri {
                    // Each value must be an array of WriteRequests. A map
                    // there is a structure where none is expected, and a
                    // string or a number is refused with no message (captured
                    // eu-west-2 and us-east-1, 11 and 12 November 2026). A
                    // null is refused once the request is read.
                    if val.is_object() {
                        return Err(crate::DynoxideError::SerializationException(
                            "Start of structure or map found where not expected".to_string(),
                        ));
                    }
                    if !val.is_array() && !val.is_null() {
                        return Err(array_not_expected());
                    }
                    if let Some(items) = val.as_array() {
                        // Check array elements are structs (WriteRequest)
                        for item in items {
                            if item.is_array() {
                                return Err(array_not_expected());
                            }
                            if !item.is_object() && !item.is_null() {
                                return Err(crate::DynoxideError::SerializationException(
                                    "Unexpected value type in payload".to_string(),
                                ));
                            }
                        }
                        for item in items {
                            if let Some(inner) = item.as_object() {
                                check_field_is_struct(inner, "DeleteRequest")?;
                                check_field_is_struct(inner, "PutRequest")?;
                                if let Some(dr) =
                                    inner.get("DeleteRequest").and_then(|v| v.as_object())
                                {
                                    check_field_is_map(dr, "Key")?;
                                    check_map_values_are_structs(dr, "Key")?;
                                }
                                if let Some(pr) =
                                    inner.get("PutRequest").and_then(|v| v.as_object())
                                {
                                    check_field_is_map(pr, "Item")?;
                                    check_map_values_are_structs(pr, "Item")?;
                                }
                            }
                        }
                    }
                }
            }
        }
        "TagResource" => {
            check_field_is_list(obj, "Tags")?;
            check_list_elements_are_structs(obj, "Tags")?;
        }
        "UpdateTimeToLive"
        | "BatchExecuteStatement"
        | "ExecuteTransaction"
        | "TransactGetItems" => refuse_arrays(operation, obj)?,
        _ => {}
    }

    // Common map fields: checked AFTER operation-specific nested fields
    check_field_is_map(obj, "Key")?;
    check_field_is_map(obj, "Item")?;
    check_field_is_map(obj, "ExpressionAttributeNames")?;
    check_field_is_map(obj, "ExpressionAttributeValues")?;
    check_field_is_map(obj, "Expected")?;

    // Check that attribute value map entries are structs (not scalars)
    check_map_values_are_structs(obj, "Key")?;
    check_map_values_are_structs(obj, "Item")?;
    check_map_values_are_structs(obj, "ExpressionAttributeValues")?;
    check_map_values_are_structs(obj, "ExclusiveStartKey")?;
    check_map_values_are_structs(obj, "Expected")?;

    // Check Expected.Attr inner fields
    if let Some(serde_json::Value::Object(expected)) = obj.get("Expected") {
        for (_attr, cond) in expected {
            if let Some(cond_obj) = cond.as_object() {
                check_field_is_bool(cond_obj, "Exists")?;
            }
        }
    }

    // Common scalar fields: checked AFTER nested fields to match DynamoDB ordering
    check_field_is_string(obj, "TableName")?;
    check_field_is_string(obj, "IndexName")?;
    check_field_is_string(obj, "ReturnConsumedCapacity")?;
    check_field_is_string(obj, "ReturnValues")?;
    check_field_is_string(obj, "ReturnItemCollectionMetrics")?;
    check_field_is_string(obj, "ConditionalOperator")?;
    check_field_is_string(obj, "Select")?;
    check_field_is_string(obj, "ConditionExpression")?;
    check_field_is_string(obj, "FilterExpression")?;
    check_field_is_string(obj, "KeyConditionExpression")?;
    check_field_is_string(obj, "ProjectionExpression")?;
    check_field_is_string(obj, "UpdateExpression")?;
    check_field_is_int(obj, "Limit")?;
    check_field_is_int(obj, "Segment")?;
    check_field_is_int(obj, "TotalSegments")?;
    check_field_is_bool(obj, "ScanIndexForward")?;
    check_field_is_bool(obj, "ConsistentRead")?;

    Ok(())
}

/// Check that a field, if present, is a JSON number (integer).
/// `java_type` is "Long" for PT fields, "Integer" for Limit/Segment/etc.
fn check_field_is_integer_typed(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    java_type: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_number() {
        return Ok(());
    }

    if val.is_array() {
        return Err(array_not_expected());
    }
    let msg = if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else if val.is_boolean() {
        if val.as_bool() == Some(true) {
            format!("TRUE_VALUE cannot be converted to {java_type}")
        } else {
            format!("FALSE_VALUE cannot be converted to {java_type}")
        }
    } else if val.is_string() {
        format!("STRING_VALUE cannot be converted to {java_type}")
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check integer field using "Long" type (for PT fields).
fn check_field_is_integer(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    check_field_is_integer_typed(obj, field, "Long")
}

/// Check integer field using "Integer" type (for Limit, Segment, etc.).
fn check_field_is_int(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    check_field_is_integer_typed(obj, field, "Integer")
}

/// Check that a field, if present and not null, is a JSON string.
/// Returns SerializationException for wrong types.
fn check_field_is_string(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_string() {
        return Ok(());
    }

    if val.is_array() {
        return Err(array_not_expected());
    }
    let msg = if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else if val.as_bool() == Some(true) {
        "TRUE_VALUE cannot be converted to String".to_string()
    } else if val.as_bool() == Some(false) {
        "FALSE_VALUE cannot be converted to String".to_string()
    } else if val.is_number() {
        // DynamoDB distinguishes DECIMAL_VALUE (float) from NUMBER_VALUE (int)
        if val.is_f64() && !val.is_i64() && !val.is_u64() {
            "DECIMAL_VALUE cannot be converted to String".to_string()
        } else {
            "NUMBER_VALUE cannot be converted to String".to_string()
        }
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check that a field, if present and not null, reads as a boolean: JSON
/// `true` or `false`, or a string DynamoDB reads as one, by the rule a `BOOL`
/// member follows (captured eu-west-2 and us-east-1, 2026-10-10).
/// Returns SerializationException for wrong types.
fn check_field_is_bool(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_boolean() || val.as_str().is_some_and(crate::types::reads_as_flag) {
        return Ok(());
    }

    if val.is_array() {
        return Err(array_not_expected());
    }
    let msg = if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else if val.is_string() {
        "Unexpected token received from parser".to_string()
    } else if val.is_number() {
        if val.is_f64() && !val.is_i64() && !val.is_u64() {
            "DECIMAL_VALUE cannot be converted to Boolean".to_string()
        } else {
            "NUMBER_VALUE cannot be converted to Boolean".to_string()
        }
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check that all elements in a list field are JSON objects (structs).
/// Returns "Unexpected value type in payload" for a scalar element, and no
/// message for an array.
fn check_list_elements_are_structs(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    if let Some(serde_json::Value::Array(arr)) = obj.get(field) {
        for item in arr {
            if item.is_array() {
                return Err(array_not_expected());
            }
            if !item.is_object() && !item.is_null() {
                return Err(crate::DynoxideError::SerializationException(
                    "Unexpected value type in payload".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Check scalar fields inside a ProvisionedThroughput struct.
fn check_nested_pt_fields(obj: &serde_json::Map<String, serde_json::Value>) -> crate::Result<()> {
    if let Some(pt) = obj.get("ProvisionedThroughput").and_then(|v| v.as_object()) {
        check_field_is_integer(pt, "WriteCapacityUnits")?;
        check_field_is_integer(pt, "ReadCapacityUnits")?;
    }
    Ok(())
}

/// Check scalar fields inside a Projection struct.
fn check_nested_projection_fields(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> crate::Result<()> {
    if let Some(proj) = obj.get("Projection").and_then(|v| v.as_object()) {
        check_field_is_string(proj, "ProjectionType")?;
    }
    Ok(())
}

/// Check that elements inside a list field are structs, and check their scalar fields.
fn check_nested_list_structs(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    if let Some(serde_json::Value::Array(arr)) = obj.get(field) {
        for item in arr {
            if let Some(inner) = item.as_object() {
                // Common struct fields in KeySchema/AttributeDefinitions elements
                check_field_is_string(inner, "KeyType")?;
                check_field_is_string(inner, "AttributeName")?;
                check_field_is_string(inner, "AttributeType")?;
                check_field_is_string(inner, "IndexName")?;
            }
        }
    }
    Ok(())
}

/// Check that elements inside a string list field are actually strings.
fn check_nested_list_strings(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    if let Some(serde_json::Value::Array(arr)) = obj.get(field) {
        for item in arr {
            if !item.is_string() && !item.is_null() {
                if item.is_boolean() {
                    let val = if item.as_bool() == Some(true) {
                        "TRUE_VALUE"
                    } else {
                        "FALSE_VALUE"
                    };
                    return Err(crate::DynoxideError::SerializationException(format!(
                        "{val} cannot be converted to String"
                    )));
                } else if item.is_number() {
                    return Err(crate::DynoxideError::SerializationException(
                        "NUMBER_VALUE cannot be converted to String".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Check that all values in a map field (if present) are JSON objects (attribute value structs).
///
/// A `Condition` DynamoDB reads by position has been rewritten to an object
/// before this runs (see [`positional`]), so any array left here is refused.
fn check_map_values_are_structs(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    if let Some(serde_json::Value::Object(map)) = obj.get(field) {
        for (_key, val) in map {
            if val.is_array() {
                return Err(array_not_expected());
            }
            if !val.is_object() && !val.is_null() {
                return Err(crate::DynoxideError::SerializationException(
                    "Unexpected value type in payload".to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// Check that a field, if present and not null, is a JSON object (map).
/// Returns SerializationException for the wrong type.
fn check_field_is_map(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_object() {
        return Ok(());
    }

    if val.is_array() {
        return Err(array_not_expected());
    }
    // Scalar value where map expected → DynamoDB returns "Unexpected field type"
    Err(crate::DynoxideError::SerializationException(
        "Unexpected field type".to_string(),
    ))
}

/// The structures, outside those the checks above type one by one, where an
/// array is refused with no message, by operation and path: a member name
/// for a structure member, `[]` after it for each element of a list of
/// structures, and `.` between steps. The request decoders are derived, and a
/// derived decoder reads a structure from an array of the right length by
/// position, so these checks, not the decoder, are what refuse it.
/// `ReplicaUpdates` and UpdateTable's `AttributeDefinitions` are not read at
/// all; DynamoDB refuses an array there all the same. TransactWriteItems is
/// absent: its own reader (`actions::transact_write_items::wire`) refuses an
/// array in its items, apart from an empty one given as an item, which
/// DynamoDB reads as an item naming no action.
const ARRAYS_REFUSED: &[(&str, &str)] = &[
    ("CreateTable", "StreamSpecification"),
    ("CreateTable", "SSESpecification"),
    ("CreateTable", "OnDemandThroughput"),
    ("CreateTable", "Tags[]"),
    ("UpdateTable", "StreamSpecification"),
    ("UpdateTable", "SSESpecification"),
    ("UpdateTable", "OnDemandThroughput"),
    ("UpdateTable", "AttributeDefinitions[]"),
    ("UpdateTable", "ReplicaUpdates[]"),
    ("UpdateTimeToLive", "TimeToLiveSpecification"),
    ("BatchExecuteStatement", "Statements[]"),
    ("ExecuteTransaction", "TransactStatements[]"),
    ("TransactGetItems", "TransactItems[]"),
    ("TransactGetItems", "TransactItems[].Get"),
];

/// Refuse an array at any of `operation`'s paths in [`ARRAYS_REFUSED`]. A
/// value of any other type is left for the checks above and the decoder.
fn refuse_arrays(
    operation: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> crate::Result<()> {
    let holds_array = |path: &str| {
        let mut values: Vec<&serde_json::Value> = Vec::new();
        for (i, step) in path.split('.').enumerate() {
            let (member, each) = match step.strip_suffix("[]") {
                Some(member) => (member, true),
                None => (step, false),
            };
            let found: Vec<&serde_json::Value> = if i == 0 {
                obj.get(member).into_iter().collect()
            } else {
                values
                    .iter()
                    .filter_map(|value| value.get(member))
                    .collect()
            };
            values = if each {
                found
                    .into_iter()
                    .filter_map(serde_json::Value::as_array)
                    .flatten()
                    .collect()
            } else {
                found
            };
            if values.iter().any(|value| value.is_array()) {
                return true;
            }
        }
        false
    };
    let refused = ARRAYS_REFUSED
        .iter()
        .any(|&(op, path)| op == operation && holds_array(path));
    if refused {
        return Err(array_not_expected());
    }
    Ok(())
}

/// Check that a field, if present and not null, is a JSON object (struct).
/// Returns SerializationException with the appropriate message for the wrong type.
fn check_field_is_struct(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_object() {
        return Ok(());
    }

    if val.is_array() {
        return Err(array_not_expected());
    }
    // Scalar value where struct expected
    Err(crate::DynoxideError::SerializationException(
        "Unexpected field type".to_string(),
    ))
}

/// Check that a field, if present and not null, is a JSON array.
/// Returns the appropriate SerializationException message for the wrong type.
fn check_field_is_list(
    obj: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> crate::Result<()> {
    let val = match obj.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return Ok(()),
    };

    if val.is_array() {
        return Ok(());
    }

    let msg = if val.is_object() {
        "Start of structure or map found where not expected".to_string()
    } else {
        "Unexpected field type".to_string()
    };

    Err(crate::DynoxideError::SerializationException(msg))
}

/// Check scalar fields inside filter condition map entries (QueryFilter/ScanFilter/KeyConditions).
fn check_filter_inner_fields(
    obj: &serde_json::Map<String, serde_json::Value>,
    filter_field: &str,
) -> crate::Result<()> {
    let filter = match obj.get(filter_field) {
        Some(v) if v.is_object() => v.as_object().unwrap(),
        _ => return Ok(()),
    };

    for (_attr_name, condition) in filter {
        if let Some(cond_obj) = condition.as_object() {
            check_field_is_string(cond_obj, "ComparisonOperator")?;
            check_field_is_list(cond_obj, "AttributeValueList")?;
            // Check AVL elements are attr structs
            if let Some(serde_json::Value::Array(avl)) = cond_obj.get("AttributeValueList") {
                for item in avl {
                    if item.is_array() {
                        return Err(array_not_expected());
                    }
                    if !item.is_object() && !item.is_null() {
                        return Err(crate::DynoxideError::SerializationException(
                            "Unexpected value type in payload".to_string(),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Check AttributeValueList fields inside a filter map (QueryFilter/ScanFilter).
///
/// The filter is a map of attribute names to condition objects, each of which
/// may contain an AttributeValueList that must be an array.
fn check_filter_attribute_value_lists(
    obj: &serde_json::Map<String, serde_json::Value>,
    filter_field: &str,
) -> crate::Result<()> {
    let filter = match obj.get(filter_field) {
        Some(v) if v.is_object() => v.as_object().unwrap(),
        _ => return Ok(()),
    };

    for (_attr_name, condition) in filter {
        if let Some(cond_obj) = condition.as_object() {
            check_field_is_list(cond_obj, "AttributeValueList")?;
        }
    }

    Ok(())
}
