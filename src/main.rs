use anyhow::{Context, Result, bail};
use indoc::indoc;
use rand::{Rng, SeedableRng};
use sqlx::migrate::MigrateDatabase;

use sqlx::sqlite::{Sqlite, SqlitePool};
use tokio::time::{self, Duration};

const DB_URL: &str = "sqlite://tasks.db";

#[derive(sqlx::FromRow, Debug)]
struct Task {
    #[allow(dead_code)]
    name: String,
    state: TaskState,
}

#[derive(sqlx::Type, Debug)]
enum TaskState {
    Waiting,
    Queued,
    Complete,
}

impl Task {
    fn new(s: &str) -> Self {
        Task {
            name: s.to_string(),
            state: TaskState::Waiting,
        }
    }
}

#[derive(Clone)]
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

    // WARN: this imposes a limit on result size, it is NOT exhaustive
    async fn get_by_state(&mut self, state: TaskState) -> Result<Vec<Task>> {
        let pending_limit = 5;
        let pending: Vec<Task> = sqlx::query_as("SELECT * FROM tasks WHERE state = $1 LIMIT $2")
            .bind(state)
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

trait Gate: Send {
    fn should_proceed(&mut self, task: &Task) -> Result<bool>;
}

struct RandomGate {
    rng: rand::rngs::StdRng,
}

impl Default for RandomGate {
    fn default() -> Self {
        Self {
            rng: rand::rngs::StdRng::from_os_rng(),
        }
    }
}

impl Gate for RandomGate {
    fn should_proceed(&mut self, _: &Task) -> Result<bool> {
        Ok(self.rng.random_bool(1.0 / 2.0))
    }
}

struct FlakyGate {
    rng: rand::rngs::StdRng,
}

impl Default for FlakyGate {
    fn default() -> Self {
        Self {
            rng: rand::rngs::StdRng::from_os_rng(),
        }
    }
}

impl Gate for FlakyGate {
    fn should_proceed(&mut self, _: &Task) -> Result<bool> {
        if self.rng.random_bool(1.0 / 5.0) {
            bail!("oops, looks like I failed!")
        }
        Ok(true)
    }
}

struct GateEvaluator {
    gates: Vec<Box<dyn Gate>>,
    multiqueue: MultiQueue,
}

impl GateEvaluator {
    async fn run(&mut self) -> Result<()> {
        let mut interval = time::interval(Duration::from_millis(1000));
        loop {
            interval.tick().await;

            let waiting = self
                .multiqueue
                .get_by_state(TaskState::Waiting)
                .await
                .context("couldn't get waiting")?;
            println!("{} tasks currently waiting", waiting.len());

            let mut to_be_queued = vec![];
            for task in waiting {
                let all_gates_proceeding: Result<bool> = self
                    .gates
                    .iter_mut()
                    .map(|gate| gate.should_proceed(&task))
                    .try_fold(true, |acc: bool, task: Result<bool>| {
                        let b = task?;
                        Ok(acc && b)
                    });

                match all_gates_proceeding {
                    Ok(true) => {
                        println!("{} eligible for transition", &task.name);
                        to_be_queued.push(task);
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

            for task in to_be_queued {
                self.multiqueue
                    .transition(&task.name, TaskState::Queued)
                    .await?;
                println!("{} enqueued", &task.name);
            }
        }
    }
}

struct TaskRunner {
    multiqueue: MultiQueue,
}

impl TaskRunner {
    async fn run(&mut self) -> Result<()> {
        let mut interval = time::interval(Duration::from_millis(1500));
        loop {
            interval.tick().await;

            let queued = self
                .multiqueue
                .get_by_state(TaskState::Queued)
                .await
                .context("couldn't get enqueued")?;

            for task in queued {
                println!("Task {} is being completed!", &task.name);

                self.multiqueue
                    .transition(&task.name, TaskState::Complete)
                    .await?;
                println!("{} transitioned to complete", &task.name);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let a = Task::new("a");
    let b = Task::new("b");
    let c = Task::new("c");

    let mut multiqueue = MultiQueue::new(vec![a, b, c])
        .await
        .context("failed to create DB")?;

    let task = multiqueue.get_by_name("a").await?;
    println!("Got: {:#?}", task);
    let task = multiqueue.get_by_name("a").await?;
    println!("Got: {:#?}", task);
    let task = multiqueue.get_by_name("c").await?;
    println!("Got: {:#?}", task);

    let gates: Vec<Box<dyn Gate>> = vec![
        Box::new(RandomGate::default()),
        Box::new(FlakyGate::default()),
    ];

    let mut evaluator = GateEvaluator {
        gates,
        multiqueue: multiqueue.clone(),
    };
    let mut runner = TaskRunner { multiqueue };

    let evaluator_handle = tokio::spawn(async move { evaluator.run().await });
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let (evaluator_result, runner_result) = tokio::join!(evaluator_handle, runner_handle);

    evaluator_result??;
    runner_result??;

    Ok(())
}
