use indoc::indoc;
use sqlx::migrate::MigrateDatabase;
use std::error::Error;

use sqlx::sqlite::{Sqlite, SqlitePool};

const DB_URL: &str = "sqlite://tasks.db";

#[derive(sqlx::FromRow, Debug)]
struct Task {
    #[allow(dead_code)]
    name: String,
}

impl Task {
    fn new(s: &str) -> Self {
        Task {
            name: s.to_string(),
        }
    }
}

struct MultiQueue {
    pool: SqlitePool,
}

impl MultiQueue {
    async fn new(tasks: Vec<Task>) -> Result<Self, Box<dyn Error>> {
        if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
            println!("[TEMP] Creating database..");
            Sqlite::create_database(DB_URL).await?;
            println!("[TEMP] Database created!");
        }
        let pool = SqlitePool::connect(DB_URL).await.unwrap();

        sqlx::query(indoc! {"
            CREATE TABLE IF NOT EXISTS tasks (
                name TEXT NOT NULL PRIMARY KEY
            )
        "})
        .execute(&pool)
        .await?;

        for task in tasks.iter() {
            let result = sqlx::query("INSERT OR IGNORE INTO tasks (name) VALUES ($1)")
                .bind(&task.name)
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

    async fn get_by_name(&mut self, name: &str) -> Result<Option<Task>, Box<dyn Error>> {
        let task: Option<Task> = sqlx::query_as("SELECT name FROM tasks WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;

        Ok(task)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let a = Task::new("a");
    let b = Task::new("b");
    let c = Task::new("c");

    let mut q = MultiQueue::new(vec![a, b, c]).await.unwrap();

    let task = q.get_by_name("a").await?;
    println!("Got: {:?}", task);
    let task = q.get_by_name("a").await?;
    println!("Got: {:?}", task);

    Ok(())
}
