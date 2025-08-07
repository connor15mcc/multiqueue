use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result, anyhow};
use multiqueue::{
    gates::{Gate, GateEvaluator},
    multiqueue::MultiQueue,
    tasks::Task,
};

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

struct PausedGate {}

impl Gate for PausedGate {
    fn should_proceed(&mut self, _: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Ok(false);
    }
}

struct FailureGate {}

impl Gate for FailureGate {
    fn should_proceed(&mut self, _: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Err(anyhow!("failure!"));
    }
}

struct AdversarialGate {
    fail_names: HashSet<String>,
}

impl Gate for AdversarialGate {
    fn should_proceed(&mut self, task: &multiqueue::tasks::Task) -> anyhow::Result<bool> {
        return Ok(!self.fail_names.contains(&task.name));
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
        gates: vec![Box::new(PausedGate {})],
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
        gates: vec![Box::new(FailureGate {})],
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
async fn test_first_paused() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let mut evaluator = GateEvaluator {
        gates: vec![Box::new(AdversarialGate {
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
