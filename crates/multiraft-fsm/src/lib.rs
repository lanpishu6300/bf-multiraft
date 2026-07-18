//! Pluggable state machine for multiraft.

mod counter_fsm;
pub use counter_fsm::CounterFsm;

pub type GroupId = u64;
pub type NodeId = u64;

#[derive(Debug, Clone, Default)]
pub struct ApplyOut {
    pub effects: Vec<u8>,
}

pub trait StateMachine: Send + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn apply(
        &mut self,
        group: GroupId,
        index: u64,
        data: &[u8],
    ) -> Result<ApplyOut, Self::Error>;

    fn snapshot(&self, group: GroupId) -> Result<Vec<u8>, Self::Error>;

    fn restore(&mut self, group: GroupId, snapshot: &[u8]) -> Result<(), Self::Error>;
}
