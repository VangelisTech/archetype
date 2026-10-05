//! Process-resource leases only; WorldManager remains the world/job inventory.
use crate::operations::Resources;
use anyhow::{Result, anyhow, ensure};
use ddlog_runtime::worlds::WorldShutdown;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct Gate {
    closing: bool,
    closed: bool,
    closer: bool,
    active: usize,
}
pub struct Host {
    resources: Mutex<Option<Arc<Resources>>>,
    shutdown: Option<WorldShutdown>,
    gate: Mutex<Gate>,
    changed: Condvar,
}
pub struct Lease {
    pub resources: Option<Arc<Resources>>,
    host: Arc<Host>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.resources.take());
        let mut gate = self.host.gate.lock().unwrap_or_else(|e| e.into_inner());
        gate.active -= 1;
        self.host.changed.notify_all();
    }
}
impl Host {
    pub fn new(resources: Resources) -> Result<Self> {
        let shutdown = resources.shutdown()?;
        Ok(Self {
            resources: Mutex::new(Some(Arc::new(resources))),
            shutdown,
            gate: Mutex::new(Gate::default()),
            changed: Condvar::new(),
        })
    }
    pub fn enter(self: &Arc<Self>) -> Result<Lease> {
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| anyhow!("Lifecycle poisoned"))?;
        ensure!(!gate.closing && !gate.closed, "Host is closing or closed");
        let resources = self
            .resources
            .lock()
            .map_err(|_| anyhow!("Resources poisoned"))?
            .clone()
            .ok_or_else(|| anyhow!("Host closed"))?;
        gate.active += 1;
        Ok(Lease {
            resources: Some(resources),
            host: self.clone(),
        })
    }
    pub fn poison(&self) {
        {
            let mut gate = self.gate.lock().unwrap_or_else(|e| e.into_inner());
            gate.closing = true;
            self.changed.notify_all();
        }
        if let Some(shutdown) = &self.shutdown {
            shutdown.stop_all();
        }
    }
    pub fn close(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        {
            let mut gate = self.gate.lock().unwrap_or_else(|e| e.into_inner());
            while gate.closer && !gate.closed {
                let left = deadline.saturating_duration_since(Instant::now());
                ensure!(
                    !left.is_zero(),
                    "Close drain timed out; ownership retained, call close again"
                );
                gate = self
                    .changed
                    .wait_timeout(gate, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            if gate.closed {
                return Ok(());
            }
            gate.closing = true;
            gate.closer = true;
        }
        // Always release the elected closer, including an unwind. Other closers
        // can inspect/retry retained ownership; mutable calls remain rejected.
        struct Closing<'a>(&'a Host);
        impl Drop for Closing<'_> {
            fn drop(&mut self) {
                let mut g = self.0.gate.lock().unwrap_or_else(|e| e.into_inner());
                g.closer = false;
                self.0.changed.notify_all();
            }
        }
        let _closing = Closing(self);
        if let Some(shutdown) = &self.shutdown {
            shutdown.stop_all();
        } // independent of manager and active-call locks
        {
            let mut gate = self.gate.lock().unwrap_or_else(|e| e.into_inner());
            while gate.active != 0 {
                let left = deadline.saturating_duration_since(Instant::now());
                ensure!(
                    !left.is_zero(),
                    "Close drain timed out; ownership retained, call close again"
                );
                gate = self
                    .changed
                    .wait_timeout(gate, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
        }
        let resources = self
            .resources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(resources) = resources {
            // Poisoned manager state cannot be safely resumed or reported as a
            // clean close. Retain it after independent process cancellation.
            let inventory = resources.inventory()?;
            for world in inventory["worlds"]
                .as_array()
                .ok_or_else(|| anyhow!("Invalid upstream inventory"))?
            {
                let id = world["id"]
                    .as_str()
                    .ok_or_else(|| anyhow!("Missing native id"))?;
                resources.manager()?.stop(id).map_err(|e| anyhow!(e))?;
            }
            loop {
                let inventory = resources.inventory()?;
                let worlds = inventory["worlds"]
                    .as_array()
                    .ok_or_else(|| anyhow!("Invalid upstream inventory"))?;
                if worlds.iter().all(|w| {
                    !matches!(w["state"].as_str(), Some("starting" | "stopping"))
                        && w["external_publication"]["busy"] != true
                }) {
                    // stop persists current state with a Result; historical
                    // checkpoint_error is advisory and may remain after repair.
                    for world in worlds {
                        let id = world["id"]
                            .as_str()
                            .ok_or_else(|| anyhow!("Missing native id"))?;
                        resources.manager()?.stop(id).map_err(|e| anyhow!(e))?;
                    }
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "Native drain timed out; ownership retained, call close again"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            drop(resources);
        }
        let resources = self
            .resources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        drop(resources); // must release roots BEFORE reporting success
        self.gate.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
        Ok(())
    }
}
