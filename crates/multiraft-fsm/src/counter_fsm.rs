use crate::{ApplyOut, GroupId, StateMachine};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CounterError {
    #[error("decode: {0}")]
    Decode(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Cmd {
    idem: u64,
    delta: i64,
}

#[derive(Debug, Default)]
pub struct CounterFsm {
    values: HashMap<GroupId, i64>,
    seen: HashMap<GroupId, HashSet<u64>>,
}

impl CounterFsm {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn encode_add(delta: i64, idem: u64) -> Vec<u8> {
        serde_json::to_vec(&Cmd { idem, delta }).unwrap()
    }

    pub fn value(&self, group: GroupId) -> i64 {
        *self.values.get(&group).unwrap_or(&0)
    }
}

impl StateMachine for CounterFsm {
    type Error = CounterError;

    fn apply(
        &mut self,
        group: GroupId,
        _index: u64,
        data: &[u8],
    ) -> Result<ApplyOut, Self::Error> {
        let cmd: Cmd = serde_json::from_slice(data)
            .map_err(|e| CounterError::Decode(e.to_string()))?;
        let seen = self.seen.entry(group).or_default();
        if seen.insert(cmd.idem) {
            *self.values.entry(group).or_default() += cmd.delta;
        }
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error> {
        let v = self.value(group);
        let seen: Vec<u64> = self
            .seen
            .get(&group)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();
        Ok(serde_json::to_vec(&(v, seen)).unwrap())
    }

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error> {
        let (v, seen): (i64, Vec<u64>) = serde_json::from_slice(snapshot)
            .map_err(|e| CounterError::Decode(e.to_string()))?;
        self.values.insert(group, v);
        self.seen.insert(group, seen.into_iter().collect());
        Ok(())
    }
}
