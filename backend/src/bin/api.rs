use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use multiqueue::{
    api::{self, AppState},
    gates::{DynamicRateLimitGate, Gate, GateEvaluator},
    multiqueue::MultiQueue,
    tasks::{Task, TaskRunner, Tier},
};
use nonzero_ext::nonzero;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let multiqueue = MultiQueue::default().await.context("create DB")?;

    let t0_gate = {
        let mut age_limits = BTreeMap::new();
        age_limits.insert(60, nonzero!(1u32));
        age_limits.insert(120, nonzero!(3u32));
        age_limits.insert(240, nonzero!(8u32));
        DynamicRateLimitGate::new(age_limits, |task: &Task| {
            task.tier == Tier::try_from(0).unwrap()
        })
    };
    let t1_gate = {
        let mut age_limits = BTreeMap::new();
        age_limits.insert(30, nonzero!(2u32));
        age_limits.insert(60, nonzero!(4u32));
        age_limits.insert(120, nonzero!(10u32));
        DynamicRateLimitGate::new(age_limits, |task: &Task| {
            task.tier == Tier::try_from(1).unwrap()
        })
    };
    let t2_gate = {
        let mut age_limits = BTreeMap::new();
        age_limits.insert(0, nonzero!(2u32));
        age_limits.insert(20, nonzero!(5u32));
        age_limits.insert(90, nonzero!(15u32));
        DynamicRateLimitGate::new(age_limits, |task: &Task| {
            task.tier == Tier::try_from(2).unwrap()
        })
    };
    let t3_gate = {
        let mut age_limits = BTreeMap::new();
        age_limits.insert(0, nonzero!(4u32));
        age_limits.insert(30, nonzero!(9u32));
        age_limits.insert(120, nonzero!(25u32));
        DynamicRateLimitGate::new(age_limits, |task: &Task| {
            task.tier == Tier::try_from(3).unwrap()
        })
    };

    let gates: Vec<Box<dyn Gate>> = vec![
        Box::new(t0_gate),
        Box::new(t1_gate),
        Box::new(t2_gate),
        Box::new(t3_gate),
    ];

    let mut evaluator = GateEvaluator {
        gates,
        multiqueue: multiqueue.clone(),
        poll_interval: Duration::from_millis(1000),
    };
    let evaluator_handle = tokio::spawn(async move { evaluator.run().await });

    let mut runner = TaskRunner::new(multiqueue.clone(), Duration::from_millis(1500));
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let server_addr = "0.0.0.0:3000";
    tracing::info!("Starting HTTP server on {}", server_addr);

    let api_state = AppState::new(multiqueue);
    let server_handle = tokio::spawn(async move { api::serve(api_state, server_addr).await });

    let (evaluator, runner, server) = tokio::join!(evaluator_handle, runner_handle, server_handle);
    evaluator??;
    runner??;
    server??;

    Ok(())
}
