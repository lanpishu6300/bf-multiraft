//! MultiRaft orchestration facade.
//!
//! The concrete `MultiRaft` type lives in `multiraft-net` (`use multiraft_net::MultiRaft`)
//! because that crate already owns the shared Router and in-process node harness,
//! while `multiraft-net` depends on `multiraft-core` for [`TypeConfig`](crate::TypeConfig).
//! Putting the facade in core would create a dependency cycle.
