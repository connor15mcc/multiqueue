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
                last_evaluated_ts INTEGER NOT NULL
            )
        "})
        .execute(&pool)
        .await?;

        Ok(MultiQueue { pool })
    }

    pub async fn insert(&mut self, tasks: Vec<Task>) -> Result<()> {
        for task in tasks.iter() {
            let result = sqlx::query(indoc! {"
                INSERT
                    OR IGNORE INTO tasks (name, state, worker_id, priority, last_evaluated_ts)
                VALUES
                    ($1, $2, $3, $4, unixepoch())
                "})
            .bind(&task.name)
            .bind(&task.state)
            .bind(&task.worker_id)
            .bind(&task.priority)
            .execute(&self.pool)
            .await?;
            match result.rows_affected() {
                0 => println!("{:?} already exists in DB", task),
                1 => println!("{:?} inserted into DB", task),
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    // WARN: this imposes a limit on result size, it is NOT exhaustive
    pub async fn get_by_state(&mut self, state: TaskState) -> Result<Vec<Task>> {
        let pending_limit = 5;
        let pending: Vec<Task> = sqlx::query_as(indoc! {"
            SELECT name, state, worker_id, priority
            FROM tasks
            WHERE state = $1
            AND worker_id IS NULL
            ORDER BY priority DESC, last_evaluated_ts ASC
            LIMIT $2
            "})
        .bind(state)
        .bind(pending_limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(pending)
    }

    pub async fn count_with_state(&mut self, state: TaskState) -> Result<u64> {
        let pending: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM tasks WHERE state = $1")
            .bind(state)
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
                name, state, worker_id, priority
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
            SET state = $1, last_evaluated_ts = unixepoch()
            WHERE name = $2
            "})
        .bind(to)
        .bind(task.name)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
