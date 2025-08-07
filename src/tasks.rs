use anyhow::{Context, Result};
use tokio::time;
use uuid::Uuid;

use crate::multiqueue::MultiQueue;

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct Task {
    #[allow(dead_code)]
    pub name: String,
    pub state: TaskState,
    pub worker_id: Option<String>,
}

#[derive(sqlx::Type, Debug, Clone)]
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
            worker_id: None,
        }
    }
}

/// A RAII wrapper for task locks that automatically releases the lock when dropped. This ensures
/// that locks are released even if the task processing fails or the worker crashes.
pub struct TaskLock {
    pub task: Task,
    multiqueue: MultiQueue,
}

impl TaskLock {
    pub async fn try_acquire(
        task: &Task,
        multiqueue: MultiQueue,
        worker_id: &str,
    ) -> Result<Option<Self>> {
        let acquired_task = multiqueue
            .claim_task_for_worker(&task.name, worker_id)
            .await?;

        if let Some(acquired_task) = acquired_task {
            Ok(Some(Self {
                task: acquired_task,
                multiqueue,
            }))
        } else {
            Ok(None)
        }
    }
}

impl Drop for TaskLock {
    fn drop(&mut self) {
        // When the TaskLock is dropped (e.g., due to error or going out of scope),
        // spawn a task to release the lock by clearing the worker_id
        let task_name = self.task.name.clone();
        let multiqueue = self.multiqueue.clone();

        tokio::spawn(async move {
            if let Err(e) = multiqueue.release_task_lock(&task_name).await {
                eprintln!("Failed to release task lock in Drop: {:?}", e);
            }
        });
    }
}

pub struct TaskRunner {
    pub multiqueue: MultiQueue,
    pub poll_interval: time::Duration,
    pub worker_id: String,
}

impl TaskRunner {
    pub fn new(multiqueue: MultiQueue, poll_interval: time::Duration) -> Self {
        let worker_id = Uuid::new_v4().to_string();
        TaskRunner {
            multiqueue,
            poll_interval,
            worker_id,
        }
    }

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

                // Try to acquire a lock for this task using the RAII TaskLock
                if let Some(_task_lock) =
                    TaskLock::try_acquire(&task, self.multiqueue.clone(), &self.worker_id).await?
                {
                    println!("{} is being completed!", name);

                    self.multiqueue
                        .transition(task, TaskState::Complete)
                        .await?;
                    println!("{} transitioned to complete", name);
                } else {
                    println!(
                        "Worker {} failed to acquire lock for task {}",
                        self.worker_id, name
                    );
                }
            }
        }
    }
}

pub struct TaskObserver {
    pub multiqueue: MultiQueue,
    pub poll_interval: time::Duration,
}

impl TaskObserver {
    pub fn new(multiqueue: MultiQueue, poll_interval: time::Duration) -> Self {
        Self {
            multiqueue,
            poll_interval,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut interval = time::interval(self.poll_interval);
        loop {
            interval.tick().await;

            let waiting_count = self.multiqueue.count_with_state(TaskState::Waiting).await?;
            let queued_count = self.multiqueue.count_with_state(TaskState::Queued).await?;
            let completed_count = self
                .multiqueue
                .count_with_state(TaskState::Complete)
                .await?;
            let total_count = waiting_count + queued_count + completed_count;

            if total_count == 0 {
                println!("Tasks: None");
                continue;
            }

            // Print state distribution on a single line
            println!(
                "Tasks: {} total [W: {} ({:.1}%), Q: {} ({:.1}%), C: {} ({:.1}%)]",
                total_count,
                waiting_count,
                100.0 * waiting_count as f64 / total_count as f64,
                queued_count,
                100.0 * queued_count as f64 / total_count as f64,
                completed_count,
                100.0 * completed_count as f64 / total_count as f64
            );
        }
    }
}
