//! Row to typed struct conversion for SQL results
//!
//! Converts sqlx AnyRow instances to typed SqlRow structs with proper type handling
//! for PostgreSQL, MySQL, and SQLite.

use crate::error::DatabaseError;
use kodegen_mcp_schema::database::{SqlRow, SqlColumnValue, SqlValue};
use sqlx::{Column, Row, TypeInfo};

/// Convert a sqlx Row to a typed SqlRow structure
///
/// Maps SQL types to the SqlValue enum for type-safe representation.
/// Handles all major database types: PostgreSQL, MySQL, SQLite.
///
/// # Arguments
/// * `row` - sqlx AnyRow to convert
///
/// # Returns
/// Typed SqlRow with column names and values
///
/// # Errors
/// Returns error if column type conversion fails
pub fn row_to_typed(row: &sqlx::any::AnyRow) -> Result<SqlRow, DatabaseError> {
    let mut columns = Vec::new();

    for column in row.columns() {
        let ordinal = column.ordinal();
        let name = column.name().to_string();
        let type_name = column.type_info().name();

        // Match on database type names and convert to SqlValue
        let value = match type_name {
            // Text types (most databases)
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" | "BPCHAR" | "NAME" | "CITEXT" => {
                match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(s)) => SqlValue::Text(s),
                    Ok(None) => SqlValue::Null,
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
                Ok(Some(v)) => SqlValue::Int(v),
                Ok(None) => SqlValue::Null,
                Err(e) => {
                    return Err(DatabaseError::QueryError(format!(
                        "Failed to extract column '{}' as INTEGER: {}",
                        name, e
                    )));
                }
            },
            // Boolean types
            "BOOLEAN" | "BOOL" | "TINYINT(1)" => match row.try_get::<Option<bool>, _>(ordinal) {
                Ok(Some(b)) => SqlValue::Bool(b),
                Ok(None) => SqlValue::Null,
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
                    Ok(Some(v)) => SqlValue::Float(v),
                    Ok(None) => SqlValue::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as FLOAT: {}",
                            name, e
                        )));
                    }
                }
            }
            // DECIMAL/NUMERIC - try as f64 first, fall back to string
            "NUMERIC" | "DECIMAL" | "NUMBER" => {
                match row.try_get::<Option<f64>, _>(ordinal) {
                    Ok(Some(v)) => SqlValue::Float(v),
                    Ok(None) => SqlValue::Null,
                    Err(_) => {
                        // If f64 fails, try as string to preserve precision
                        match row.try_get::<Option<String>, _>(ordinal) {
                            Ok(Some(s)) => SqlValue::Text(s),
                            Ok(None) => SqlValue::Null,
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
            // JSON types - store as text (already JSON-formatted)
            "JSON" | "JSONB" => {
                match row.try_get::<Option<String>, _>(ordinal) {
                    Ok(Some(json_str)) => SqlValue::Text(json_str),
                    Ok(None) => SqlValue::Null,
                    Err(e) => {
                        return Err(DatabaseError::QueryError(format!(
                            "Failed to extract column '{}' as JSON: {}",
                            name, e
                        )));
                    }
                }
            }
            // Binary types - store as Vec<u8>
            "BYTEA" | "BLOB" | "BINARY" | "VARBINARY" => {
                match row.try_get::<Option<Vec<u8>>, _>(ordinal) {
                    Ok(Some(bytes)) => SqlValue::Blob(bytes),
                    Ok(None) => SqlValue::Null,
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
                    Ok(Some(s)) => SqlValue::Text(s),
                    Ok(None) => SqlValue::Null,
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
                Ok(Some(s)) => SqlValue::Text(s),
                Ok(None) => SqlValue::Null,
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

        columns.push(SqlColumnValue { name, value });
    }

    Ok(SqlRow { columns })
}
