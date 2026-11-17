use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use indexmap::IndexMap;

use crate::io_debugger::log::Value;

#[derive(Debug, Default)]
pub struct Matcher {
    pub task_id: Option<u64>,
    pub block_id: Option<u64>,
    pub fun: Option<&'static str>,
    pub args: IndexMap<&'static str, Value>,
}

#[derive(Debug)]
pub struct Call<'a> {
    pub task_id: Option<u64>,
    pub block_id: Option<u64>,
    pub fun: &'static str,
    pub args: &'a IndexMap<&'static str, Value>,
}

impl Call<'_> {
    fn matches(&self, matcher: &Matcher) -> bool {
        let Matcher {
            task_id,
            block_id,
            fun,
            args,
        } = matcher;

        task_id
            .map(|task_id| Some(task_id) == self.task_id)
            .unwrap_or(true)
            && block_id
                .map(|block_id| Some(block_id) == self.block_id)
                .unwrap_or(true)
            && fun.map(|fun| fun == self.fun).unwrap_or(true)
            && args.iter().all(|(k, v)| Some(v) == self.args.get(k))
    }
}

#[derive(Debug, Default)]
pub struct Order {
    matches: Mutex<Vec<Option<Matcher>>>,
    next_idx: Arc<(Mutex<usize>, Condvar)>,
}

impl Order {
    pub fn arrange(&self, call: Call<'_>) -> ArrangeGuard<'_> {
        let mut matches = self.matches.lock().unwrap();
        let Some((idx, matcher)) = matches.iter_mut().enumerate().find(|(_idx, matcher)| {
            matcher
                .as_ref()
                .map(|matcher| call.matches(matcher))
                .unwrap_or_default()
        }) else {
            // no match found
            return ArrangeGuard { inner: None };
        };

        // wipe matcher
        *matcher = None;

        // unlock matchers
        drop(matches);

        // wait for our turn
        let (next_idx, condvar) = self.next_idx.as_ref();
        let guard = condvar
            .wait_while(next_idx.lock().unwrap(), |next_idx| *next_idx < idx)
            .unwrap();

        ArrangeGuard {
            inner: Some((idx, guard, condvar)),
        }
    }
}

impl Drop for Order {
    fn drop(&mut self) {
        let has_data = {
            let guard = self.matches.lock().unwrap();
            guard.iter().any(|m| m.is_some())
        };

        if has_data && !std::thread::panicking() {
            panic!("matches left");
        }
    }
}

impl<const N: usize> From<[Matcher; N]> for Order {
    fn from(matches: [Matcher; N]) -> Self {
        Self {
            matches: Mutex::new(matches.into_iter().map(Some).collect()),
            next_idx: Default::default(),
        }
    }
}

pub struct ArrangeGuard<'a> {
    inner: Option<(usize, MutexGuard<'a, usize>, &'a Condvar)>,
}

impl Drop for ArrangeGuard<'_> {
    fn drop(&mut self) {
        if let Some((idx, mut guard, condvar)) = self.inner.take() {
            *guard = idx + 1;
            drop(guard);
            condvar.notify_all();
        }
    }
}
