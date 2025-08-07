use anyhow::{Context, Result};

use crate::{
    gates::{FlakyGate, Gate, GateEvaluator, RandomGate},
    multiqueue::MultiQueue,
    task::{Task, TaskRunner},
};

mod gates;
mod multiqueue;
mod task;

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
    };
    let mut runner = TaskRunner { multiqueue };

    let evaluator_handle = tokio::spawn(async move { evaluator.run().await });
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let (evaluator_result, runner_result) = tokio::join!(evaluator_handle, runner_handle);

    evaluator_result??;
    runner_result??;

    Ok(())
}
