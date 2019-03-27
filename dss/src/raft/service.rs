labrpc::service! {
    service raft {
        rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
        rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);
        // Your code here if more rpc desired.
        // rpc xxx(yyy) returns (zzz)
    }
}
pub use self::raft::{
    add_service as add_raft_service, Client as RaftClient, Service as RaftService,
};

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(int64, tag = "1")]
    pub term: i64,
    #[prost(int64, tag = "2")]
    pub candidate_id: i64,
}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(int64, tag = "1")]
    pub term: i64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesArgs {
    #[prost(int64, tag = "1")]
    pub term: i64,
    #[prost(int64, tag = "2")]
    pub leader_id: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesReply {
    #[prost(int64, tag = "1")]
    pub term: i64,
}
