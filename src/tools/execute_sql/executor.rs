//! SQL execution logic for ExecuteSQL tool
//!
//! Provides single and multi-statement execution with transaction support.

use crate::{
    DatabaseType, tools::timeout::execute_with_timeout,
};
use super::row_converter::row_to_typed;
use kodegen_mcp_tool::error::McpError;
use kodegen_config_manager::ConfigManager;
use kodegen_mcp_schema::database::{ExecuteSQLOutput, SqlStatementError, SqlRow};
use sqlx::{AnyPool, Row, Column};
use std::sync::Arc;
use std::time::Duration;

/// ExecuteSQL tool struct with connection pool and configuration
#[derive(Clone)]
pub struct ExecuteSQLTool {
    pub(crate) pool: Arc<AnyPool>,
    pub(crate) config: ConfigManager,
    pub(crate) db_type: DatabaseType,
}

impl ExecuteSQLTool {
    /// Create a new ExecuteSQL tool instance
    ///
    /// # Arguments
    /// * `pool` - Shared connection pool
    /// * `config` - Configuration manager
    /// * `connection_url` - Database connection URL for type detection
    ///
    /// # Errors
    /// Returns error if connection_url cannot be parsed to determine database type
    pub fn new(
        pool: Arc<AnyPool>,
        config: ConfigManager,
        connection_url: &str,
    ) -> Result<Self, McpError> {
        let db_type = DatabaseType::from_url(connection_url)
            .map_err(|e| anyhow::anyhow!("Failed to determine database type: {}", e))?;
        Ok(Self {
            pool,
            config,
            db_type,
        })
    }

    /// Get database type from stored field
    pub fn get_database_type(&self) -> Result<DatabaseType, McpError> {
        Ok(self.db_type)
    }

    /// Execute a single SQL statement
    ///
    /// # Arguments
    /// * `sql` - SQL statement to execute
    ///
    /// # Returns
    /// Typed ExecuteSQLOutput with rows and row_count
    pub async fn execute_single(&self, sql: &str) -> Result<ExecuteSQLOutput, McpError> {
        // Execute query with timeout
        let pool = self.pool.clone();
        let sql_owned = sql.to_string();
        let rows = execute_with_timeout(
            &self.config,
            "db_query_timeout_secs",
            Duration::from_secs(60), // 60s default for data queries
            || {
                let pool = pool.clone();
                let sql = sql_owned.clone();
                async move { sqlx::query(&sql).fetch_all(&*pool).await }
            },
            &format!(
                "Executing SQL: {}",
                sql.chars().take(50).collect::<String>()
            ),
        )
        .await?;

        // Extract column names
        let columns = extract_column_names(&rows);

        // Convert rows to typed SqlRow structures
        let typed_rows: Vec<SqlRow> = rows
            .iter()
            .map(|row| row_to_typed(row).map_err(|e| anyhow::anyhow!("{}", e)))
            .collect::<Result<_, _>>()?;

        let row_count = typed_rows.len();

        Ok(ExecuteSQLOutput {
            columns,
            rows: typed_rows,
            row_count,
            affected_rows: None,
            execution_time_ms: 0, // Caller will set this in mod.rs
            executed_statements: None,
            total_statements: None,
            errors: None,
        })
    }

    /// Execute multiple SQL statements within a transaction
    ///
    /// Returns partial results if execution fails partway through.
    /// All changes are rolled back on error - no data is committed.
    ///
    /// # Arguments
    /// * `statements` - SQL statements to execute atomically
    ///
    /// # Returns
    /// Typed ExecuteSQLOutput with execution statistics
    pub async fn execute_multi_transactional(&self, statements: &[String]) -> Result<ExecuteSQLOutput, McpError> {
        // Begin transaction with timeout
        let pool = self.pool.clone();
        let mut tx = execute_with_timeout(
            &self.config,
            "db_query_timeout_secs",
            Duration::from_secs(30),
            || {
                let pool = pool.clone();
                async move { pool.begin().await }
            },
            "Starting transaction",
        )
        .await?;
        
        let mut all_rows: Vec<SqlRow> = Vec::new();
        let mut all_columns: Vec<String> = Vec::new();
        let mut executed_statements = 0;

        for (index, statement) in statements.iter().enumerate() {
            // Execute each statement with timeout
            let timeout_duration = self
                .config
                .get_value("db_query_timeout_secs")
                .and_then(|v| match v {
                    kodegen_config_manager::ConfigValue::Number(n) => {
                        Some(Duration::from_secs(n as u64))
                    }
                    _ => None,
                })
                .unwrap_or(Duration::from_secs(60));

            let rows_result = match tokio::time::timeout(
                timeout_duration,
                sqlx::query(statement).fetch_all(&mut *tx),
            )
            .await
            {
                Ok(Ok(rows)) => Ok(rows),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(sqlx::Error::PoolTimedOut),
            };

            match rows_result {
                Ok(rows) => {
                    executed_statements += 1;
                    if !rows.is_empty() {
                        // Extract columns from first result set if not yet set
                        if all_columns.is_empty() {
                            all_columns = extract_column_names(&rows);
                        }
                        
                        // Convert rows to typed structures
                        for row in &rows {
                            let typed_row = row_to_typed(row)
                                .map_err(|e| anyhow::anyhow!("{}", e))?;
                            all_rows.push(typed_row);
                        }
                    }
                }
                Err(e) => {
                    // Rollback transaction
                    let _ = tx.rollback().await;

                    // Return error with typed structure
                    return Ok(ExecuteSQLOutput {
                        columns: vec![],
                        rows: vec![],
                        row_count: 0,
                        affected_rows: None,
                        execution_time_ms: 0,
                        executed_statements: Some(executed_statements),
                        total_statements: Some(statements.len()),
                        errors: Some(vec![SqlStatementError {
                            statement_index: index + 1,
                            statement: statement.clone(),
                            error: format!("Statement {} failed: {}. Transaction rolled back. No data committed.", index + 1, e),
                        }]),
                    });
                }
            }
        }

        // Commit transaction with timeout
        let timeout_duration = self
            .config
            .get_value("db_query_timeout_secs")
            .and_then(|v| match v {
                kodegen_config_manager::ConfigValue::Number(n) => Some(Duration::from_secs(n as u64)),
                _ => None,
            })
            .unwrap_or(Duration::from_secs(30));

        match tokio::time::timeout(timeout_duration, tx.commit()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(
                    crate::error::DatabaseError::QueryError(format!("Transaction commit failed: {}", e)).into(),
                );
            }
            Err(_) => {
                return Err(
                    crate::error::DatabaseError::QueryError("Transaction commit timed out".to_string()).into(),
                );
            }
        }

        let row_count = all_rows.len();
        Ok(ExecuteSQLOutput {
            columns: all_columns,
            rows: all_rows,
            row_count,
            affected_rows: None,
            execution_time_ms: 0,
            executed_statements: Some(executed_statements),
            total_statements: Some(statements.len()),
            errors: None,
        })
    }

    /// Execute multiple SQL statements WITHOUT transaction
    ///
    /// Continues execution on error, collecting all results and errors.
    /// Changes from successful statements are committed even if later statements fail.
    ///
    /// # Arguments
    /// * `statements` - SQL statements to execute independently
    ///
    /// # Returns
    /// Typed ExecuteSQLOutput with rows, errors array, and execution statistics
    pub async fn execute_multi_non_transactional(
        &self,
        statements: &[String],
    ) -> Result<ExecuteSQLOutput, McpError> {
        let mut all_rows: Vec<SqlRow> = Vec::new();
        let mut all_columns: Vec<String> = Vec::new();
        let mut errors: Vec<SqlStatementError> = Vec::new();
        let mut executed_statements = 0;

        for (index, statement) in statements.iter().enumerate() {
            // Execute each statement with timeout
            let pool = self.pool.clone();
            let statement_owned = statement.clone();
            let rows_result = execute_with_timeout(
                &self.config,
                "db_query_timeout_secs",
                Duration::from_secs(60),
                || {
                    let pool = pool.clone();
                    let stmt = statement_owned.clone();
                    async move { sqlx::query(&stmt).fetch_all(&*pool).await }
                },
                &format!(
                    "Executing: {}",
                    statement.chars().take(50).collect::<String>()
                ),
            )
            .await;

            match rows_result {
                Ok(rows) => {
                    executed_statements += 1;
                    if !rows.is_empty() {
                        // Extract columns from first result set if not yet set
                        if all_columns.is_empty() {
                            all_columns = extract_column_names(&rows);
                        }
                        
                        // Convert rows to typed structures
                        for row in &rows {
                            let typed_row = row_to_typed(row)
                                .map_err(|e| anyhow::anyhow!("{}", e))?;
                            all_rows.push(typed_row);
                        }
                    }
                }
                Err(e) => {
                    // Record error but continue execution
                    errors.push(SqlStatementError {
                        statement_index: index + 1,
                        statement: statement.clone(),
                        error: e.to_string(),
                    });
                }
            }
        }

        let row_count = all_rows.len();
        Ok(ExecuteSQLOutput {
            columns: all_columns,
            rows: all_rows,
            row_count,
            affected_rows: None,
            execution_time_ms: 0,
            executed_statements: Some(executed_statements),
            total_statements: Some(statements.len()),
            errors: if errors.is_empty() { None } else { Some(errors) },
        })
    }
}

/// Extract column names from sqlx rows
fn extract_column_names(rows: &[sqlx::any::AnyRow]) -> Vec<String> {
    if rows.is_empty() {
        return vec![];
    }
    rows[0]
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect()
}
