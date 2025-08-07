use std::time::Duration;

use anyhow::{Context, Result};
use multiqueue::{
    gates::{FlakyGate, Gate, GateEvaluator, RandomGate},
    multiqueue::MultiQueue,
    tasks::{Task, TaskObserver, TaskRunner},
};

#[tokio::main]
async fn main() -> Result<()> {
    let tasks = ('a'..='z').map(|c| Task::new(&c.to_string())).collect();

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
    let mut observer = TaskObserver::new(multiqueue, Duration::from_millis(1000));

    let evaluator_handle = tokio::spawn(async move { evaluator.run().await });
    let runner_handle = tokio::spawn(async move { runner.run().await });
    let observer_handle = tokio::spawn(async move { observer.run().await });

    let (evaluator, runner, observer) =
        tokio::join!(evaluator_handle, runner_handle, observer_handle);

    evaluator??;
    runner??;
    observer??;

    Ok(())
}
