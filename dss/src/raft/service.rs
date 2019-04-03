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

#[derive(Copy, Clone, Eq, PartialEq, Message)]
pub struct LogInfo {
    #[prost(uint64, tag = "1")]
    pub log_term: u64,
    #[prost(uint64, tag = "2")]
    pub log_index: u64,
}

// The "more up-to-date" compare
impl Ord for LogInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.log_term
            .cmp(&other.log_term)
            .then_with(|| self.log_index.cmp(&other.log_index))
    }
}

impl PartialOrd for LogInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Example RequestVote RPC arguments structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteArgs {
    // Your data here (2A, 2B).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub candidate_id: u64,
    #[prost(message, required, tag = "3")]
    pub last_log_info: LogInfo,
}

// Example RequestVote RPC reply structure.
#[derive(Clone, PartialEq, Message)]
pub struct RequestVoteReply {
    // Your data here (2A).
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct Log {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bytes, tag = "2")]
    pub command: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesArgs {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub leader_commit: u64,
    #[prost(message, required, tag = "4")]
    pub prev_log_info: LogInfo,
    #[prost(message, repeated, tag = "5")]
    pub entries: Vec<Log>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AppendEntriesReply {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
    #[prost(message, optional, tag = "3")]
    pub reject_hint: Option<LogInfo>,
}
