use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use multiqueue::{
    gates::{Gate, GateEvaluator},
    multiqueue::MultiQueue,
    tasks::{Task, TaskLock, TaskRunner, TaskState, TaskPriority},
};
use tokio::time;

/*
 * Implementation must:
 * - NOT block head of line -- support arbitrary delay in gate success for a given task
 *  - in case of logical gate "pauses"
 *  - in case of gate evaluation failure
 * - NOT lose work in case of disruption or other delay / failure
 * - NOT allow for a task to be worked on multiple times / concurrently
 *  - prefer pessimistic locking over optimistic -- delays are preferable to duplicate work
 * - provide easy observability into state machine progression and task status & "ETA"
 */

struct ReadyGate {}

impl Gate for ReadyGate {
    fn should_proceed(&mut self, _: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Ok(true);
    }
}

struct FailGate {}

impl Gate for FailGate {
    fn should_proceed(&mut self, _: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Ok(false);
    }
}

struct ErrGate {}

impl Gate for ErrGate {
    fn should_proceed(&mut self, _: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Err(anyhow!("failure!"));
    }
}

struct AdversarialFailGate {
    fail_names: HashSet<String>,
}

impl Gate for AdversarialFailGate {
    fn should_proceed(&mut self, task: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Ok(!self.fail_names.contains(&task.name));
    }
}

struct AdversarialErrGate {
    err_names: HashSet<String>,
}

impl Gate for AdversarialErrGate {
    fn should_proceed(&mut self, task: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        if self.err_names.contains(&task.name) {
            return Err(anyhow!("failure!"));
        }
        return Ok(true);
    }
}

#[tokio::test]
async fn test_all_ready() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let mut evaluator = GateEvaluator {
        gates: vec![Box::new(ReadyGate {})],
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1),
    };

    let _ = tokio::time::timeout(Duration::from_secs(1), evaluator.run()).await;

    assert_eq!(
        multiqueue
            .count_with_state(multiqueue::tasks::TaskState::Queued)
            .await?,
        26
    );

    Ok(())
}

#[tokio::test]
async fn test_all_paused() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let mut evaluator = GateEvaluator {
        gates: vec![Box::new(FailGate {})],
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1),
    };

    let _ = tokio::time::timeout(Duration::from_secs(1), evaluator.run()).await;

    assert_eq!(
        multiqueue
            .count_with_state(multiqueue::tasks::TaskState::Queued)
            .await?,
        0
    );

    Ok(())
}

#[tokio::test]
async fn test_all_failing() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let mut evaluator = GateEvaluator {
        gates: vec![Box::new(ErrGate {})],
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1),
    };

    let _ = tokio::time::timeout(Duration::from_secs(1), evaluator.run()).await;

    assert_eq!(
        multiqueue
            .count_with_state(multiqueue::tasks::TaskState::Queued)
            .await?,
        0
    );

    Ok(())
}

#[tokio::test]
async fn test_first_failing() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let mut evaluator = GateEvaluator {
        gates: vec![Box::new(AdversarialFailGate {
            fail_names: ('a'..='m').map(|c| c.to_string()).collect(),
        })],
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1),
    };

    let _ = tokio::time::timeout(Duration::from_secs(1), evaluator.run()).await;

    assert_eq!(
        multiqueue
            .count_with_state(multiqueue::tasks::TaskState::Queued)
            .await?,
        13
    );

    Ok(())
}

#[tokio::test]
async fn test_first_err() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let mut evaluator = GateEvaluator {
        gates: vec![Box::new(AdversarialErrGate {
            err_names: ('a'..='m').map(|c| c.to_string()).collect(),
        })],
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1),
    };

    let _ = tokio::time::timeout(Duration::from_secs(1), evaluator.run()).await;

    assert_eq!(
        multiqueue
            .count_with_state(multiqueue::tasks::TaskState::Queued)
            .await?,
        13
    );

    Ok(())
}

#[tokio::test]
async fn test_concurrent_task_exclusive_access() -> Result<()> {
    let task = Task::new("exclusive-task");
    let tasks = vec![task];

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert task")?;

    for task in multiqueue.get_by_state(TaskState::Waiting).await? {
        multiqueue.transition(task, TaskState::Queued).await?;
    }

    // Set up tracking for which worker processed the task
    let worker1_processed = AtomicBool::new(false);
    let worker2_processed = AtomicBool::new(false);

    let worker1_future = {
        let worker1_processed = &worker1_processed;
        let mut runner = TaskRunner::new(multiqueue.clone(), Duration::from_millis(10));

        async move {
            let queued = runner.multiqueue.get_by_state(TaskState::Queued).await?;

            for task in queued {
                // Try to acquire a lock for this task using RAII TaskLock
                if let Some(_task_lock) =
                    TaskLock::try_acquire(&task, runner.multiqueue.clone(), &runner.worker_id)
                        .await?
                {
                    // 1-second processing delay
                    time::sleep(Duration::from_secs(1)).await;

                    worker1_processed.store(true, Ordering::SeqCst);
                    runner
                        .multiqueue
                        .transition(task, TaskState::Complete)
                        .await?;
                }
            }

            Ok::<(), anyhow::Error>(())
        }
    };

    let worker2_future = {
        let worker2_processed = &worker2_processed;
        let mut runner = TaskRunner::new(multiqueue.clone(), Duration::from_millis(10));

        async move {
            // Custom run loop that only processes one batch of tasks
            let queued = runner.multiqueue.get_by_state(TaskState::Queued).await?;

            for task in queued {
                // Try to acquire a lock for this task using RAII TaskLock
                if let Some(_task_lock) =
                    TaskLock::try_acquire(&task, runner.multiqueue.clone(), &runner.worker_id)
                        .await?
                {
                    // 1-second processing delay
                    time::sleep(Duration::from_secs(1)).await;

                    worker2_processed.store(true, Ordering::SeqCst);
                    runner
                        .multiqueue
                        .transition(task, TaskState::Complete)
                        .await?;
                }
            }

            Ok::<(), anyhow::Error>(())
        }
    };

    // Run both workers concurrently
    let (worker1_result, worker2_result) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(3), worker1_future),
        tokio::time::timeout(Duration::from_secs(3), worker2_future)
    );

    // Handle potential timeouts
    worker1_result.unwrap().unwrap();
    worker2_result.unwrap().unwrap();

    // Check which workers processed the task
    let worker1_did_process = worker1_processed.load(Ordering::SeqCst);
    let worker2_did_process = worker2_processed.load(Ordering::SeqCst);

    // Assert that only one worker processed the task
    assert!(
        (worker1_did_process ^ worker2_did_process),
        "Task was processed by both workers: worker1={}, worker2={}",
        worker1_did_process,
        worker2_did_process
    );

    // Verify the task was completed exactly once
    let completed_count = multiqueue.count_with_state(TaskState::Complete).await?;
    assert_eq!(completed_count, 1);

    Ok(())
}

#[tokio::test]
async fn test_lock_release_on_error() -> Result<()> {
    let task = Task::new("error-test-task");
    let tasks = vec![task];

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert task")?;

    for task in multiqueue.get_by_state(TaskState::Waiting).await? {
        multiqueue.transition(task, TaskState::Queued).await?;
    }

    // First worker tries to process but encounters an error
    {
        let tasks = multiqueue.get_by_state(TaskState::Queued).await?;
        assert_eq!(tasks.len(), 1);

        let task = tasks.into_iter().nth(0).expect("inserted exactly one");

        // Acquire the lock
        let task_lock = TaskLock::try_acquire(&task, multiqueue.clone(), "worker-1").await?;
        assert!(task_lock.is_some(), "Worker 1 should acquire the lock");

        // Now we'll simulate an error by letting the task_lock go out of scope
        // without explicitly calling complete()
        drop(task_lock);

        // The lock should have been automatically released via Drop
    }

    // The second worker should now be able to acquire the lock and complete the task
    {
        // Small delay to ensure drop handler completes (since it spawns a task)
        time::sleep(Duration::from_millis(50)).await;

        let tasks = multiqueue.get_by_state(TaskState::Queued).await?;
        assert_eq!(tasks.len(), 1, "Task should still be available after error");

        let task = tasks.into_iter().nth(0).expect("inserted exactly one");

        // Worker 2 should be able to acquire the lock
        let task_lock = TaskLock::try_acquire(&task, multiqueue.clone(), "worker-2").await?;
        assert!(
            task_lock.is_some(),
            "Worker 2 should be able to acquire the lock after error"
        );

        // Complete the task
        if let Some(_lock) = task_lock {
            multiqueue.transition(task, TaskState::Complete).await?;
        }
    }

    // Verify the task was completed despite the first worker's error
    let completed_count = multiqueue.count_with_state(TaskState::Complete).await?;
    assert_eq!(
        completed_count, 1,
        "Task should be completed by the second worker"
    );

    Ok(())
}

#[tokio::test]
async fn test_task_priorities() -> Result<()> {
    let mut tasks = Vec::new();

    // Create low priority tasks
    tasks.push(Task::with_priority("low-1", TaskPriority::Low));
    tasks.push(Task::with_priority("low-2", TaskPriority::Low));
    tasks.push(Task::with_priority("low-3", TaskPriority::Low));
    
    // Create high priority tasks
    tasks.push(Task::with_priority("high-1", TaskPriority::High));
    tasks.push(Task::with_priority("high-2", TaskPriority::High));
    
    // Insert tasks into the queue
    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;
    
    // Transition all tasks to Queued state
    for task in multiqueue.get_by_state(TaskState::Waiting).await? {
        multiqueue.transition(task, TaskState::Queued).await?;
    }
    
    // Get tasks in Queued state - they should be ordered by priority
    let queued_tasks = multiqueue.get_by_state(TaskState::Queued).await?;
    
    // Assert that high priority tasks are returned first
    assert_eq!(queued_tasks.len(), 5);
    assert_eq!(queued_tasks[0].name, "high-1");
    assert_eq!(queued_tasks[1].name, "high-2");
    
    // The next tasks should be the low priority ones
    // (Their order among themselves is determined by insertion order/timestamp)
    assert_eq!(queued_tasks[2].name, "low-1");
    assert_eq!(queued_tasks[3].name, "low-2");
    assert_eq!(queued_tasks[4].name, "low-3");
    
    Ok(())
}
