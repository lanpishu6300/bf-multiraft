//! Cross-process tonic/gRPC transport for Multi-Raft.
//!
//! One unary [`RaftService::call`](proto::raft_service_server::RaftService) RPC
//! carries `group_id` + path + UTF-8 JSON payload (same as in-process encode/decode).

pub mod router;
pub mod server;

pub mod proto {
    tonic::include_proto!("multiraft");
}

pub use router::GrpcRouter;
pub use server::GrpcServer;
