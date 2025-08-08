use std::time::Duration;

use anyhow::{Context, Result};
use multiqueue::{
    gates::{FlakyGate, Gate, GateEvaluator, RandomGate},
    multiqueue::MultiQueue,
    tasks::{Task, TaskCanceller, TaskObserver, TaskPriority, TaskRunner},
};

#[tokio::main]
async fn main() -> Result<()> {
    let tasks = ('a'..='z')
        .enumerate()
        .map(|(i, c)| {
            if (i + 1) % 8 == 0 {
                Task::with_priority(&c.to_string(), TaskPriority::High)
            } else {
                Task::new(&c.to_string())
            }
        })
        .collect();

    let mut multiqueue = MultiQueue::default().await.context("create DB")?;
    multiqueue.insert(tasks).await.context("insert tasks")?;

    let gates: Vec<Box<dyn Gate>> = vec![
        Box::new(RandomGate::default()),
        Box::new(FlakyGate::default()),
    ];

    let mut evaluator = GateEvaluator {
        gates,
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1000),
    };
    let mut runner = TaskRunner::new(multiqueue.clone(), Duration::from_millis(1500));
    let mut observer = TaskObserver::new(multiqueue.clone(), Duration::from_millis(1000));
    let mut canceller = TaskCanceller::new(
        multiqueue.clone(),
        Duration::from_millis(1000),
        String::from("/tmp/multiqueue_cancellations"),
    );

    let evaluator_handle = tokio::spawn(async move { evaluator.run().await });
    let runner_handle = tokio::spawn(async move { runner.run().await });
    let observer_handle = tokio::spawn(async move { observer.run().await });
    let canceller_handle = tokio::spawn(async move { canceller.run().await });

    let (evaluator, runner, observer, canceller) = tokio::join!(
        evaluator_handle,
        runner_handle,
        observer_handle,
        canceller_handle
    );

    evaluator??;
    runner??;
    observer??;
    canceller??;

    Ok(())
}
