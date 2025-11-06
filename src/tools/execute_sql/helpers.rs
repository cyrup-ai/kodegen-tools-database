//! Helper functions for SQL execution
//!
//! Utility functions for determining execution strategies.

use crate::{DatabaseType, extract_first_keyword};

/// Determine if statements contain write operations requiring transaction
///
/// Analyzes SQL statements to identify write operations (INSERT, UPDATE, DELETE, etc.)
/// that should be wrapped in a transaction for atomicity.
///
/// # Arguments
/// * `statements` - SQL statements to analyze
/// * `db_type` - Database type for keyword extraction
///
/// # Returns
/// `true` if any statement is a write operation requiring transaction
pub fn should_use_transaction(statements: &[String], db_type: DatabaseType) -> bool {
    statements.iter().any(|stmt| {
        if let Ok(keyword) = extract_first_keyword(stmt, db_type) {
            matches!(
                keyword.as_str(),
                "insert" | "update" | "delete" | "create" | "alter" | "drop" | "truncate"
            )
        } else {
            // If can't parse keyword, assume write for safety
            true
        }
    })
}
