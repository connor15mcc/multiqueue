use anyhow::Result;
use indoc::indoc;
use sqlx::SqlitePool;

use crate::tasks::*;

#[derive(Clone)]
pub struct MultiQueue {
    pool: SqlitePool, /* clone on SqlitePool is tied to the same shared cxn pool */
}

impl MultiQueue {
    pub async fn default() -> Result<Self> {
        return Self::from_url(":memory:").await;
    }

    pub async fn from_url(url: &str) -> Result<Self> {
        let pool = SqlitePool::connect(url).await?;
        sqlx::query(indoc! {"
            CREATE TABLE IF NOT EXISTS tasks (
                name TEXT NOT NULL PRIMARY KEY,
                state TEXT NOT NULL,
                worker_id TEXT,
                priority INTEGER NOT NULL,
                tier INTEGER NOT NULL,
                last_evaluated_ts INTEGER NOT NULL,
                last_transitioned INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            )
        "})
        .execute(&pool)
        .await?;

        Ok(MultiQueue { pool })
    }

    pub async fn insert(&mut self, tasks: Vec<Task>) -> Result<u64> {
        let mut count = 0;
        for task in tasks.iter() {
            let result = sqlx::query(indoc! {"
                INSERT
                    OR IGNORE INTO tasks (name, state, worker_id, priority, tier, last_evaluated_ts, last_transitioned, created_at)
                VALUES
                    ($1, $2, $3, $4, $5, unixepoch(), unixepoch(), $6)
                "})
            .bind(&task.name)
            .bind(&task.state)
            .bind(&task.worker_id)
            .bind(&task.priority)
            .bind(task.tier)
            .bind(task.created_at)
            .execute(&self.pool)
            .await?;
            match result.rows_affected() {
                0 => {}
                1 => count += 1,
                _ => unreachable!(),
            }
        }

        Ok(count)
    }

    /// WARN: this imposes a limit on result size, it is NOT exhaustive
    /// this is sorted by least recently evaluated, perfect for evaluation without starvation
    pub async fn get_by_state(
        &mut self,
        state: TaskState,
        limit: Option<u32>,
    ) -> Result<Vec<Task>> {
        let default_limit = 50;
        let limit = limit.unwrap_or(default_limit).clamp(1, default_limit);
        let pending: Vec<Task> = sqlx::query_as(indoc! {"
            SELECT name, state, worker_id, priority, tier, created_at, last_transitioned
            FROM tasks
            WHERE state = $1
            AND worker_id IS NULL
            ORDER BY priority DESC, last_evaluated_ts ASC
            LIMIT $2
            "})
        .bind(state)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(pending)
    }

    // WARN: this imposes a limit on result size, it is NOT exhaustive
    /// this is sorted by transition time, perfect for display
    pub async fn view_by_state(
        &mut self,
        state: TaskState,
        limit: Option<u32>,
    ) -> Result<Vec<Task>> {
        let default_limit = 50;
        let limit = limit.unwrap_or(default_limit).clamp(1, default_limit);
        let pending: Vec<Task> = sqlx::query_as(indoc! {"
            SELECT name, state, worker_id, priority, tier, created_at, last_transitioned
            FROM tasks
            WHERE state = $1
            AND worker_id IS NULL
            ORDER BY priority DESC, last_transitioned DESC
            LIMIT $2
            "})
        .bind(state)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(pending)
    }

    pub async fn count(&mut self, state: TaskState, tier: Option<Tier>) -> Result<u64> {
        let pending: u64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tasks WHERE (state = $1 AND (tier = $2 OR $2 IS NULL))",
        )
        .bind(state)
        .bind(tier)
        .fetch_one(&self.pool)
        .await?;

        Ok(pending)
    }

    pub async fn claim_task_for_worker(
        &self,
        task_name: &str,
        worker_id: &str,
    ) -> Result<Option<Task>> {
        let mut tx = self.pool.begin().await?;

        // Check if the task is available (not claimed by another worker)
        let task: Option<Task> = sqlx::query_as(indoc! {"
            UPDATE tasks
            SET
                worker_id = $1,
                last_evaluated_ts = unixepoch()
            WHERE
                name = $2
                AND worker_id IS NULL
            RETURNING
                name, state, worker_id, priority, tier, created_at, last_transitioned
            "})
        .bind(worker_id)
        .bind(task_name)
        .fetch_optional(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(task)
    }

    pub async fn release_task_lock(&self, task_name: &str) -> Result<()> {
        sqlx::query(indoc! {"
            UPDATE tasks
            SET worker_id = NULL
            WHERE name = $1
            "})
        .bind(task_name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn transition(&mut self, task: Task, to: TaskState) -> Result<()> {
        sqlx::query(indoc! {"
            UPDATE tasks
            SET state = $1, last_transitioned = unixepoch()
            WHERE name = $2
            "})
        .bind(to)
        .bind(task.name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_evaluation_time(&mut self, task: &Task) -> Result<()> {
        sqlx::query(indoc! {"
            UPDATE tasks
            SET last_evaluated_ts = unixepoch()
            WHERE name = $1
            "})
        .bind(&task.name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn cancel_tasks<S: AsRef<str>>(&mut self, task_names: &[S]) -> Result<u64> {
        if task_names.is_empty() {
            return Ok(0);
        }

        // Use a transaction for better performance with multiple updates
        let mut tx = self.pool.begin().await?;

        let mut total_cancelled = 0;
        for name in task_names {
            let result = sqlx::query(indoc! {"
                UPDATE tasks
                SET state = $1, last_evaluated_ts = unixepoch(), last_transitioned = unixepoch()
                WHERE name = $2
                AND (state = $3 OR state = $4)
                "})
            .bind(TaskState::Cancelled)
            .bind(name.as_ref())
            .bind(TaskState::Waiting)
            .bind(TaskState::Queued)
            .execute(&mut *tx)
            .await?;

            total_cancelled += result.rows_affected();
        }

        tx.commit().await?;
        Ok(total_cancelled)
    }
}
