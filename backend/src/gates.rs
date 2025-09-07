use anyhow::{Context as _, Result, bail};
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use nonzero_ext::nonzero;
use rand::{Rng, SeedableRng};
use std::{collections::BTreeMap, num::NonZeroU32, time::SystemTime, time::UNIX_EPOCH};
use tokio::time;
use tracing::debug;

use crate::{multiqueue::MultiQueue, tasks::*};

pub trait Gate: Send {
    fn should_proceed(&mut self, task: &Task) -> Result<bool>;
}

// Removed undefined BakeGate implementation

pub struct RandomGate {
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

pub struct FlakyGate {
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

pub struct RateLimitGate<F>
where
    F: FnMut(&Task) -> bool + Send,
{
    limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    // Filter function to decide if rate limiting should apply
    filter: F,
}

impl<F> RateLimitGate<F>
where
    F: FnMut(&Task) -> bool + Send,
{
    pub fn new(tasks_per_minute: NonZeroU32, filter: F) -> Self {
        let quota = Quota::per_minute(tasks_per_minute);
        let limiter = RateLimiter::direct(quota);
        Self { limiter, filter }
    }
}

impl<F> Gate for RateLimitGate<F>
where
    F: FnMut(&Task) -> bool + Send,
{
    fn should_proceed(&mut self, task: &Task) -> Result<bool> {
        if !(self.filter)(task) {
            // Task is not subject to rate limiting
            return Ok(true);
        }

        match self.limiter.check() {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

/// A rate limiter that dynamically adjusts limits based on task age.
/// Tasks are categorized into "buckets" based on their age, and each bucket
/// has its own rate limit configuration.
pub struct DynamicRateLimitGate<F>
where
    F: FnMut(&Task) -> bool + Send,
{
    // Ordered map of time thresholds (in seconds) to rate limiters The key is the max age in
    // seconds, and the value is the rate limiter for tasks within that age
    limiters: BTreeMap<u64, RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    // Filter function to decide if rate limiting should apply
    filter: F,
}

impl<F> DynamicRateLimitGate<F>
where
    F: FnMut(&Task) -> bool + Send,
{
    pub fn new(age_limits: BTreeMap<u64, NonZeroU32>, filter: F) -> Self {
        let mut limiters = BTreeMap::new();

        for (age, limit) in age_limits {
            let quota = Quota::per_minute(limit).allow_burst(nonzero!(1u32));
            limiters.insert(age, RateLimiter::direct(quota));
        }

        Self { filter, limiters }
    }

    fn get_task_age_seconds(&self, task: &Task) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        (now - task.created_at).max(0) as u64
    }

    fn get_limiter_for_task(
        &self,
        task: &Task,
    ) -> Option<&RateLimiter<NotKeyed, InMemoryState, DefaultClock>> {
        let task_age = self.get_task_age_seconds(task);

        // Find the last entry smaller than the task age, such that bucket < task_age
        match &self.limiters.range(..task_age).last() {
            Some((_, limiter)) => Some(limiter),
            None => None,
        }
    }
}

impl<F> Gate for DynamicRateLimitGate<F>
where
    F: FnMut(&Task) -> bool + Send,
{
    fn should_proceed(&mut self, task: &Task) -> Result<bool> {
        if !(self.filter)(task) {
            return Ok(true);
        }

        let limiter = self.get_limiter_for_task(task);
        let Some(limiter) = limiter else {
            debug!("blocking as ineligible");
            return Ok(false);
        };

        match limiter.check() {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

pub struct GateEvaluator {
    pub gates: Vec<Box<dyn Gate>>,
    pub multiqueue: MultiQueue,
    pub poll_interval: time::Duration,
}

impl GateEvaluator {
    pub async fn run(&mut self) -> Result<()> {
        let mut interval = time::interval(self.poll_interval);
        loop {
            interval.tick().await;

            let waiting = self
                .multiqueue
                .get_by_state(TaskState::Waiting, None)
                .await
                .context("couldn't get waiting")?;
            debug!("{} tasks currently waiting", waiting.len());

            for task in waiting {
                let all_gates_proceeding: Result<bool> = self
                    .gates
                    .iter_mut()
                    .map(|gate| gate.should_proceed(&task))
                    .try_fold(true, |acc: bool, task: Result<bool>| {
                        let b = task?;
                        Ok(acc && b)
                    });
                self.multiqueue.update_evaluation_time(&task).await?;

                match all_gates_proceeding {
                    Ok(true) => {
                        debug!("{} eligible for transition", &task.name);
                        self.multiqueue
                            .transition(task.clone(), TaskState::Queued)
                            .await?;
                    }
                    Ok(false) => {
                        debug!(
                            "{} ineligible for transition: should not proceed",
                            &task.name
                        );
                    }
                    Err(e) => {
                        debug!(
                            "{} couldn't be evaluted for transition: {:?}",
                            &task.name, e,
                        );
                    }
                }
            }
        }
    }
}
