//! Row to JSON conversion for SQL results
//!
//! Converts sqlx AnyRow instances to JSON with proper type handling for
//! PostgreSQL, MySQL, and SQLite.

use base64::Engine as _; // For base64 encoding of binary data
use crate::error::DatabaseError;
use serde_json::{Value, json};
use sqlx::{Column, Row, TypeInfo};

/// Convert a sqlx Row to a JSON object
///
/// Dynamically extracts column names and values, converting to appropriate JSON types.
/// Handles NULL values gracefully by returning Value::Null.
///
/// # Type Name Variations
/// Type names vary by database:
/// - PostgreSQL: TEXT, INT4, INT8, BOOL, FLOAT8, etc.
/// - MySQL: VARCHAR, INT, BIGINT, TINYINT, DOUBLE, etc.
/// - SQLite: TEXT, INTEGER, REAL, BLOB, etc.
///
/// # Arguments
/// * `row` - sqlx AnyRow to convert
///
/// # Returns
/// JSON Value representing the row as an object with column names as keys
///
/// # Errors
/// Returns error if:
/// - Column type is unsupported
/// - Type conversion fails
/// - Column extraction fails
pub fn row_to_json(row: &sqlx::any::AnyRow) -> Result<Value, DatabaseError> {
    let mut map = serde_json::Map::new();

    for column in row.columns() {
        let ordinal = column.ordinal();
        let name = column.name().to_string();
        let type_name = column.type_info().name();

        // Match on database type names
        let value = match type_name {
            // Text types (most databases)
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" | "BPCHAR" | "NAME" | "CITEXT" => {
                match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(s)) => Value::String(s),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as TEXT: {}",
                            name, e
                        )));
                    }
                }
            }
            // Integer types
            "INTEGER" | "INT" | "INT2" | "INT4" | "INT8" | "BIGINT" | "SMALLINT" | "MEDIUMINT"
            | "SERIAL" | "BIGSERIAL" => match row.try_get::<Option<i64>, _>(ordinal) {
                Ok(Some(v)) => json!(v),
                Ok(None) => Value::Null,
                Err(e) => {
                    return Err(DatabaseError::QueryError(format!(
                        "Failed to extract column '{}' as INTEGER: {}",
                        name, e
                    )));
                }
            },
            // Boolean types
            "BOOLEAN" | "BOOL" | "TINYINT(1)" => match row.try_get::<Option<bool>, _>(ordinal) {
                Ok(Some(b)) => Value::Bool(b),
                Ok(None) => Value::Null,
                Err(e) => {
                    return Err(DatabaseError::QueryError(format!(
                        "Failed to extract column '{}' as BOOLEAN: {}",
                        name, e
                    )));
                }
            },
            // Float types
            "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => {
                match row.try_get::<Option<f64>, _>(ordinal) {
                    Ok(Some(v)) => json!(v),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as FLOAT: {}",
                            name, e
                        )));
                    }
                }
            }
            // DECIMAL/NUMERIC - sqlx::any doesn't support these types
            // Try as f64 first (may lose precision for very large numbers)
            "NUMERIC" | "DECIMAL" | "NUMBER" => {
                match row.try_get::<Option<f64>, _>(ordinal) {
                    Ok(Some(v)) => json!(v),
                    Ok(None) => Value::Null,
                    Err(_) => {
                        // If f64 fails, try as string
                        match row.try_get::<Option<String>, _>(ordinal) {
                            Ok(Some(s)) => Value::String(s),
                            Ok(None) => Value::Null,
                            Err(e) => {
                                return Err(DatabaseError::QueryError(format!(
                                    "Failed to extract column '{}' as DECIMAL (tried f64 and string): {}. \
                                 Consider using CAST({} AS TEXT) in your query.",
                                    name, e, name
                                )));
                            }
                        }
                    }
                }
            }
            // JSON types - parse as serde_json::Value
            "JSON" | "JSONB" => {
                match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(json_str)) => {
                        serde_json::from_str(&json_str).unwrap_or_else(|e| {
                            log::warn!("Failed to parse JSON column '{}': {}", name, e);
                            Value::String(json_str) // Fallback to raw string
                        })
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as JSON: {}",
                            name, e
                        )));
                    }
                }
            }
            // Binary types - encode as base64 string
            "BYTEA" | "BLOB" | "BINARY" | "VARBINARY" => {
                match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
                    Ok(Some(bytes)) => {
                        let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
                        json!({
                            "type": "base64",
                            "data": encoded
                        })
                    }
                    Ok(None) => Value::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as BYTEA: {}",
                            name, e
                        )));
                    }
                }
            }
            // Date/Time types - extract as strings
            "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" | "DATE" | "TIME" | "INTERVAL" => {
                match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(s)) => Value::String(s),
                    Ok(None) => Value::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as {}: {}",
                            name, type_name, e
                        )));
                    }
                }
            }
            // UUID - extract as string
            "UUID" => match row.try_get::<Option<String>, _>(ordinal) {
                Ok(Some(s)) => Value::String(s),
                Ok(None) => Value::Null,
                Err(e) => {
                    return Err(DatabaseError::QueryError(format!(
                        "Failed to extract column '{}' as UUID: {}",
                        name, e
                    )));
                }
            },
            // Fallback for unsupported types
            _ => {
                return Err(DatabaseError::QueryError(format!(
                    "Unsupported column type '{}' for column '{}'. \
                     Supported types: TEXT, VARCHAR, INTEGER, BIGINT, BOOLEAN, REAL, FLOAT, DOUBLE, \
                     NUMERIC, DECIMAL, JSON, JSONB, BYTEA, BLOB, TIMESTAMP, DATE, TIME, UUID. \
                     Consider casting this column in your query: CAST({} AS TEXT)",
                    type_name, name, name
                )));
            }
        };

        map.insert(name, value);
    }

    Ok(Value::Object(map))
}
