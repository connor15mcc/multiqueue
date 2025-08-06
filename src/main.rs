use anyhow::{Context, Result};
use indoc::indoc;
use rand::Rng;
use sqlx::migrate::MigrateDatabase;

use sqlx::sqlite::{Sqlite, SqlitePool};

const DB_URL: &str = "sqlite://tasks.db";

#[derive(sqlx::FromRow, Debug)]
struct Task {
    #[allow(dead_code)]
    name: String,
    state: TaskState,
}

#[derive(sqlx::Type, Debug)]
enum TaskState {
    Pending,
    Complete,
}

impl Task {
    fn new(s: &str) -> Self {
        Task {
            name: s.to_string(),
            state: TaskState::Pending,
        }
    }
}

struct MultiQueue {
    pool: SqlitePool,
}

impl MultiQueue {
    async fn new(tasks: Vec<Task>) -> Result<Self> {
        if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
            println!("[TEMP] Creating database..");
            Sqlite::create_database(DB_URL).await?;
            println!("[TEMP] Database created!");
        }
        let pool = SqlitePool::connect(DB_URL).await.unwrap();

        sqlx::query(indoc! {"
            CREATE TABLE IF NOT EXISTS tasks (
                name TEXT NOT NULL PRIMARY KEY,
                state TEXT NOT NULL
            )
        "})
        .execute(&pool)
        .await?;

        for task in tasks.iter() {
            let result = sqlx::query("INSERT OR IGNORE INTO tasks (name, state) VALUES ($1, $2)")
                .bind(&task.name)
                .bind(&task.state)
                .execute(&pool)
                .await?;
            match result.rows_affected() {
                0 => println!("{:?} already exists in DB", task),
                1 => println!("{:?} inserted into DB", task),
                _ => unreachable!(),
            }
        }

        Ok(MultiQueue { pool })
    }

    async fn get_by_name(&mut self, name: &str) -> Result<Option<Task>> {
        let task: Option<Task> = sqlx::query_as("SELECT * FROM tasks WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

        Ok(task)
    }

    async fn get_pending(&mut self) -> Result<Vec<Task>> {
        let pending_limit = 5;
        let pending: Vec<Task> = sqlx::query_as("SELECT * FROM tasks WHERE state = $1 LIMIT $2")
            .bind(TaskState::Pending)
            .bind(pending_limit)
            .fetch_all(&self.pool)
            .await?;

        Ok(pending)
    }

    async fn transition(&mut self, name: &str, to: TaskState) -> Result<()> {
        sqlx::query("UPDATE tasks SET state = $1 WHERE name = $2")
            .bind(to)
            .bind(name)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

trait Gate {
    fn should_proceed(&self, task: &Task) -> Result<bool>;
}

struct Random {}

impl Gate for Random {
    fn should_proceed(&self, _: &Task) -> Result<bool> {
        Ok(rand::rng().random_bool(1.0 / 2.0))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let batch_size = 5;

    let a = Task::new("a");
    let b = Task::new("b");
    let c = Task::new("c");

    let mut q = MultiQueue::new(vec![a, b, c])
        .await
        .context("failed to create DB")?;

    let task = q.get_by_name("a").await?;
    println!("Got: {:#?}", task);
    let task = q.get_by_name("a").await?;
    println!("Got: {:#?}", task);
    let task = q.get_by_name("c").await?;
    println!("Got: {:#?}", task);

    let gate = Random {};
    loop {
        let pending = q.get_pending().await.context("couldn't get pending")?;
        println!("Pending: {:#?}", pending);
        if pending.len() == 0 {
            break;
        }

        let mut transitionable = vec![];
        // WARN: consider the case where all `batch_size` pending tasks are un-transitionable --
        // will deadlock. could avoid by re-ordering those items to back
        for task in pending.iter().take(batch_size) {
            match gate.should_proceed(&task) {
                Ok(true) => {
                    println!("{} eligible for transition", &task.name);
                    transitionable.push(task);
                }
                Ok(false) => {
                    println!(
                        "{} ineligible for transition: should not proceed",
                        &task.name,
                    );
                }
                Err(e) => {
                    println!(
                        "{} couldn't be evaluted for transition: {:?}",
                        &task.name, e,
                    );
                }
            }
        }

        for task in transitionable {
            q.transition(&task.name, TaskState::Complete).await?;
            println!("{} transitioned to complete", &task.name);
        }
    }

    Ok(())
}
