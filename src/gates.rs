use anyhow::{Context as _, Result, bail};
use rand::{Rng, SeedableRng};
use tokio::time;

use crate::{multiqueue::MultiQueue, tasks::*};

pub trait Gate: Send {
    fn should_proceed(&mut self, task: &Task) -> Result<bool>;
}

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
                .get_by_state(TaskState::Waiting)
                .await
                .context("couldn't get waiting")?;
            println!("{} tasks currently waiting", waiting.len());

            for task in waiting {
                let all_gates_proceeding: Result<bool> = self
                    .gates
                    .iter_mut()
                    .map(|gate| gate.should_proceed(&task))
                    .try_fold(true, |acc: bool, task: Result<bool>| {
                        let b = task?;
                        Ok(acc && b)
                    });

                // HACK: we "transition" back to `Waiting` (it's current state) if we can't
                // transition to `Queued` so as to update the timestamp and put at the end of the
                // line. This could eventually simply update the ts in the unhappy case, rather
                // than a full (no-op) transition
                let mut next = TaskState::Waiting;
                match all_gates_proceeding {
                    Ok(true) => {
                        println!("{} eligible for transition", &task.name);
                        next = TaskState::Queued;
                    }
                    Ok(false) => {
                        println!(
                            "{} ineligible for transition: should not proceed",
                            &task.name
                        );
                    }
                    Err(e) => {
                        println!(
                            "{} couldn't be evaluted for transition: {:?}",
                            &task.name, e,
                        );
                    }
                }
                self.multiqueue.transition(task, next).await?;
            }
        }
    }
}
