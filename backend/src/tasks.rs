use anyhow::{Context, Result, bail};
use tokio::time;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{multiqueue::MultiQueue, proto};

#[derive(sqlx::Type, Debug, Clone, PartialEq, Default)]
#[repr(i32)]
pub enum TaskPriority {
    #[default]
    Low = 0,
    High = 1,
}

impl From<proto::multiqueue::task::TaskPriority> for TaskPriority {
    fn from(proto: proto::multiqueue::task::TaskPriority) -> Self {
        use proto::multiqueue::task::TaskPriority;

        match proto {
            TaskPriority::Low => Self::Low,
            TaskPriority::High => Self::High,
        }
    }
}

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq)]
#[sqlx(transparent)]
pub struct Tier(i32);

impl Default for Tier {
    fn default() -> Self {
        Self(3)
    }
}

impl TryFrom<i32> for Tier {
    type Error = anyhow::Error;

    fn try_from(i: i32) -> Result<Self, Self::Error> {
        if (0..=3).contains(&i) {
            return Ok(Self(i));
        }
        bail!("invalid tier: {}", i)
    }
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct Task {
    #[allow(dead_code)]
    pub name: String,
    pub state: TaskState,
    pub worker_id: Option<String>,
    pub priority: TaskPriority,
    pub tier: Tier,
    pub created_at: i64,
    pub last_transitioned: i64,
}

impl TryFrom<proto::multiqueue::SubmitTaskRequest> for Task {
    type Error = anyhow::Error;

    fn try_from(proto: proto::multiqueue::SubmitTaskRequest) -> Result<Self> {
        let priority = proto::multiqueue::task::TaskPriority::try_from(proto.priority)
            .unwrap_or_default()
            .into();
        let tier = Tier(proto.tier);

        let t = Task::with_priority_and_tier(proto.name, priority, tier)?;
        Ok(t)
    }
}

impl Into<proto::multiqueue::Task> for Task {
    fn into(self) -> proto::multiqueue::Task {
        proto::multiqueue::Task {
            name: self.name,
            state: Into::<TaskState>::into(self.state) as i32,
            priority: Into::<TaskPriority>::into(self.priority) as i32,
            created_at: self.created_at,
            last_transitioned: self.last_transitioned,
            tier: self.tier.0,
        }
    }
}

impl Task {
    pub fn new(s: String) -> Result<Self> {
        if s.trim().is_empty() {
            bail!("cannot have empty task name");
        }

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        Ok(Task {
            name: s,
            state: TaskState::default(),
            worker_id: None,
            priority: TaskPriority::default(),
            tier: Tier::default(),
            created_at: current_time,
            last_transitioned: current_time,
        })
    }

    pub fn with_priority(s: String, priority: TaskPriority) -> Result<Self> {
        let mut t = Task::new(s)?;
        t.priority = priority;
        Ok(t)
    }

    pub fn with_priority_and_tier(s: String, priority: TaskPriority, tier: Tier) -> Result<Self> {
        let mut t = Task::with_priority(s, priority)?;
        t.tier = tier;
        Ok(t)
    }
}

#[derive(sqlx::Type, Debug, Clone, PartialEq, Default)]
pub enum TaskState {
    #[default]
    Waiting,
    Queued,
    Complete,
    Cancelled,
}

impl From<proto::multiqueue::TaskState> for TaskState {
    fn from(proto: proto::multiqueue::TaskState) -> Self {
        use proto::multiqueue::TaskState;

        match proto {
            TaskState::Waiting => Self::Waiting,
            TaskState::Queued => Self::Queued,
            TaskState::Complete => Self::Complete,
            TaskState::Cancelled => Self::Cancelled,
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
                .get_by_state(TaskState::Queued, None)
                .await
                .context("couldn't get enqueued")?;

            for task in queued {
                let name = task.name.to_owned();

                // Try to acquire a lock for this task using the RAII TaskLock
                if let Some(_task_lock) =
                    TaskLock::try_acquire(&task, self.multiqueue.clone(), &self.worker_id).await?
                {
                    info!("{} is being completed!", name);

                    self.multiqueue
                        .transition(task, TaskState::Complete)
                        .await?;
                    info!("{} transitioned to complete", name);
                } else {
                    warn!(
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

            let waiting_count = self.multiqueue.count(TaskState::Waiting, None).await?;
            let queued_count = self.multiqueue.count(TaskState::Queued, None).await?;
            let completed_count = self.multiqueue.count(TaskState::Complete, None).await?;
            let cancelled_count = self.multiqueue.count(TaskState::Cancelled, None).await?;
            let total_count = waiting_count + queued_count + completed_count + cancelled_count;

            if total_count == 0 {
                println!("Tasks: None");
                continue;
            }

            println!(
                "Tasks: {} total [W: {} ({:.1}%), Q: {} ({:.1}%), C: {} ({:.1}%), X: {} ({:.1}%)]",
                total_count,
                waiting_count,
                100.0 * waiting_count as f64 / total_count as f64,
                queued_count,
                100.0 * queued_count as f64 / total_count as f64,
                completed_count,
                100.0 * completed_count as f64 / total_count as f64,
                cancelled_count,
                100.0 * cancelled_count as f64 / total_count as f64
            );
        }
    }
}

pub struct TaskCanceller {
    pub multiqueue: MultiQueue,
    pub poll_interval: time::Duration,
    pub pipe_path: String,
}

impl TaskCanceller {
    pub fn new(multiqueue: MultiQueue, poll_interval: time::Duration, pipe_path: String) -> Self {
        Self {
            multiqueue,
            poll_interval,
            pipe_path,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut interval = time::interval(self.poll_interval);
        loop {
            interval.tick().await;

            let path = std::path::Path::new(&self.pipe_path);
            if !path.exists() {
                continue;
            }

            let content = tokio::fs::read_to_string(&self.pipe_path).await?;
            let tasks_to_cancel: Vec<&str> =
                content.lines().filter(|l| !l.trim().is_empty()).collect();

            if tasks_to_cancel.is_empty() {
                continue;
            }

            println!(
                "Received cancellation request for tasks: {:?}",
                tasks_to_cancel
            );

            let count = self.multiqueue.cancel_tasks(&tasks_to_cancel).await?;
            println!("Cancelled {} tasks", count);

            // Remove the pipe after processing to avoid reprocessing the same data
            tokio::fs::remove_file(&self.pipe_path).await?;
        }
    }
}
