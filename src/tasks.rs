use anyhow::{Context, Result};
use tokio::time;

use crate::multiqueue::MultiQueue;

#[derive(sqlx::FromRow, Debug)]
pub struct Task {
    #[allow(dead_code)]
    pub name: String,
    pub state: TaskState,
}

#[derive(sqlx::Type, Debug)]
pub enum TaskState {
    Waiting,
    Queued,
    Complete,
}

impl Task {
    pub fn new(s: &str) -> Self {
        Task {
            name: s.to_string(),
            state: TaskState::Waiting,
        }
    }
}

pub struct TaskRunner {
    pub multiqueue: MultiQueue,
    pub poll_interval: time::Duration,
}

impl TaskRunner {
    pub async fn run(&mut self) -> Result<()> {
        let mut interval = time::interval(self.poll_interval);
        loop {
            interval.tick().await;

            let queued = self
                .multiqueue
                .get_by_state(TaskState::Queued)
                .await
                .context("couldn't get enqueued")?;

            for task in queued {
                let name = task.name.to_owned();
                println!("Task {} is being completed!", name);

                self.multiqueue
                    .transition(task, TaskState::Complete)
                    .await?;
                println!("{} transitioned to complete", name);
            }
        }
    }
}
