//! GetPoolStats tool - Exposes connection pool health metrics

use crate::DatabaseType;
use kodegen_mcp_schema::{Tool, ToolExecutionContext, ToolResponse, McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{GetPoolStatsArgs, GetPoolStatsOutput, ConnectionStats, PoolConfiguration, PoolHealth, PoolStatsPrompts};
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
    type Prompts = PoolStatsPrompts;

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

    async fn execute(&self, _args: Self::Args, _ctx: ToolExecutionContext) 
        -> Result<ToolResponse<<Self::Args as ToolArgs>::Output>, McpError> 
    {
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

        // Human-readable display
        let display = format!(
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
        
        // Create typed output with nested structs
        let output = GetPoolStatsOutput {
            database_type: format!("{:?}", self.db_type),
            connections: ConnectionStats {
                total: size,
                active: num_active,
                idle: num_idle,
            },
            configuration: PoolConfiguration {
                max_connections,
                min_connections: options.get_min_connections(),
                acquire_timeout_secs: options.get_acquire_timeout().as_secs(),
                idle_timeout_secs: options.get_idle_timeout().map(|d| d.as_secs()),
                max_lifetime_secs: options.get_max_lifetime().map(|d| d.as_secs()),
                test_before_acquire: options.get_test_before_acquire(),
            },
            health: PoolHealth {
                status: health_status.to_string(),
                utilization_pct,
            },
        };
        
        Ok(ToolResponse::new(display, output))
    }
}
