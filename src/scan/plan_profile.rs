use std::cell::RefCell;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Bucket {
    label: &'static str,
    calls: u64,
    total: Duration,
}

#[derive(Default)]
struct State {
    active: bool,
    depth: u32,
    started: Option<Instant>,
    buckets: Vec<Bucket>,
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

pub(crate) struct Scope {
    label: &'static str,
    started: Option<Instant>,
}

pub(crate) fn begin() -> bool {
    STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        if state.active {
            state.depth += 1;
            return false;
        }
        state.active = crate::PROFILE_PLANNING.get();
        state.depth = u32::from(state.active);
        state.started = state.active.then(Instant::now);
        state.buckets.clear();
        state.active
    })
}

pub(crate) fn scope(label: &'static str) -> Scope {
    let active = STATE.with(|cell| cell.borrow().active);
    Scope {
        label,
        started: active.then(Instant::now),
    }
}

pub(crate) fn record(label: &'static str, elapsed: Duration) {
    STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        if !state.active {
            return;
        }
        if let Some(bucket) = state.buckets.iter_mut().find(|b| b.label == label) {
            bucket.calls += 1;
            bucket.total += elapsed;
            return;
        }
        state.buckets.push(Bucket {
            label,
            calls: 1,
            total: elapsed,
        });
    });
}

pub(crate) fn count(label: &'static str) {
    record(label, Duration::ZERO);
}

pub(crate) fn finish() {
    STATE.with(|cell| {
        let mut state = cell.borrow_mut();
        if !state.active {
            return;
        }
        if state.depth > 1 {
            state.depth -= 1;
            return;
        }
        let total = state.started.map(|t| t.elapsed()).unwrap_or_default();
        let mut parts = Vec::with_capacity(state.buckets.len() + 1);
        parts.push(format!("total={:.3}ms", total.as_secs_f64() * 1000.0));
        for bucket in &state.buckets {
            parts.push(format!(
                "{}={:.3}ms/{}",
                bucket.label,
                bucket.total.as_secs_f64() * 1000.0,
                bucket.calls
            ));
        }
        pgrx::notice!("pg_deltax planning profile: {}", parts.join(" "));
        state.active = false;
        state.depth = 0;
        state.started = None;
        state.buckets.clear();
    });
}

impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            record(self.label, started.elapsed());
        }
    }
}
