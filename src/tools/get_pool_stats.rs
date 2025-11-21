//! GetPoolStats tool - Exposes connection pool health metrics

use crate::DatabaseType;
use kodegen_mcp_tool::{Tool, ToolExecutionContext, error::McpError};
use kodegen_mcp_schema::database::{GetPoolStatsArgs, GetPoolStatsPromptArgs};
use rmcp::model::{Content, PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};
use serde_json::json;
use sqlx::AnyPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct GetPoolStatsTool {
    pool: Arc<AnyPool>,
    db_type: DatabaseType,
}

impl GetPoolStatsTool {
    pub fn new(pool: Arc<AnyPool>, connection_url: &str) -> Result<Self, McpError> {
        let db_type = DatabaseType::from_url(connection_url)
            .map_err(|e| anyhow::anyhow!("Failed to determine database type: {}", e))?;
        Ok(Self { pool, db_type })
    }
}

impl Tool for GetPoolStatsTool {
    type Args = GetPoolStatsArgs;
    type PromptArgs = GetPoolStatsPromptArgs;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_POOL_STATS
    }

    fn description() -> &'static str {
        "Get connection pool health metrics including active connections, \
         idle connections, and pool configuration. Use this to diagnose \
         connection pool exhaustion or monitor database connection health."
    }

    fn read_only() -> bool {
        true // Read-only operation
    }

    async fn execute(&self, _args: Self::Args, _ctx: ToolExecutionContext) -> Result<Vec<Content>, McpError> {
        // Get pool metrics
        let size = self.pool.size();
        let num_idle = self.pool.num_idle();
        let num_active = size.saturating_sub(num_idle as u32);

        // Get pool options
        let options = self.pool.options();
        
        // Calculate health metrics
        let max_connections = options.get_max_connections();
        let health_status = if num_active == max_connections {
            "EXHAUSTED"
        } else if num_idle == 0 {
            "BUSY"
        } else {
            "HEALTHY"
        };
        let utilization_pct = (num_active as f64 / max_connections as f64 * 100.0).round() as u32;

        let mut contents = Vec::new();
        
        // Human-readable summary
        let summary = format!(
            "🔌 Connection Pool Health\n\n\
             Status: {}\n\
             Utilization: {}%\n\
             Active: {}/{}\n\
             Idle: {}",
            health_status,
            utilization_pct,
            num_active,
            max_connections,
            num_idle
        );
        contents.push(Content::text(summary));
        
        // JSON metadata
        let metadata = json!({
            "database_type": format!("{:?}", self.db_type),
            "connections": {
                "total": size,
                "active": num_active,
                "idle": num_idle,
            },
            "configuration": {
                "max_connections": max_connections,
                "min_connections": options.get_min_connections(),
                "acquire_timeout_secs": options.get_acquire_timeout().as_secs(),
                "idle_timeout_secs": options.get_idle_timeout().map(|d| d.as_secs()),
                "max_lifetime_secs": options.get_max_lifetime().map(|d| d.as_secs()),
                "test_before_acquire": options.get_test_before_acquire(),
            },
            "health": {
                "status": health_status,
                "utilization_pct": utilization_pct,
            }
        });
        let json_str = serde_json::to_string_pretty(&metadata)
            .unwrap_or_else(|_| "{}".to_string());
        contents.push(Content::text(json_str));
        
        Ok(contents)
    }

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![]
    }

    async fn prompt(&self, _args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::text(
                    "How do I check the database connection pool health?",
                ),
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::text(
                    "Use get_pool_stats to inspect connection pool health:\n\n\
                     get_pool_stats({})\n\n\
                     Returns:\n\
                     • connections.total: Current number of connections\n\
                     • connections.active: Connections in use\n\
                     • connections.idle: Connections available\n\
                     • configuration: Pool settings (max_connections, timeouts, etc.)\n\
                     • health.status: HEALTHY | BUSY | EXHAUSTED\n\
                     • health.utilization_pct: Percentage of pool in use\n\n\
                     If status is EXHAUSTED (all connections in use), queries will wait \
                     for available connections up to acquire_timeout. Consider increasing \
                     max_connections if this occurs frequently.",
                ),
            },
        ])
    }
}
