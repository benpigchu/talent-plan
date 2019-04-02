use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time;

use futures::future::Either;
use futures::sync::{mpsc, oneshot};
use futures::{Future, Stream};
use futures_timer::Delay;
use labcodec;
use labrpc::RpcFuture;
use rand::{Rng, ThreadRng};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Clone, Debug)]
struct DetailedState {
    state: State,
    expected_log_length: u64,
}

/// The current role of the node
#[derive(PartialEq, Debug)]
enum RoleState {
    Follower,
    Candidate,
    Leader,
}

/// type of messages recieved from other
#[derive(Debug)]
enum Incoming {
    RequestVote(RequestVoteArgs, oneshot::Sender<RequestVoteReply>),
    AppendEntries(AppendEntriesArgs, oneshot::Sender<AppendEntriesReply>),
    Vote(u64, RequestVoteReply),
    Feedback(u64, AppendEntriesReply),
    Log(Vec<u8>),
}

///Message to the raft thread
#[derive(Debug)]
enum Command {
    Inbound(Incoming),
    Kill,
}

fn push_inbound(sender: &mpsc::UnboundedSender<Command>, incoming: Incoming) {
    sender
        .unbounded_send(Command::Inbound(incoming))
        .unwrap_or_default();
}

/// type of timer timeout
#[derive(Debug)]
enum TimeoutType {
    Heartbeat,
    Election,
}

/// type of task to be processed
#[derive(Debug)]
enum Task {
    Packet(Incoming),
    Timeout(TimeoutType),
}

/// infomation related to timers
#[derive(Debug)]
struct Timing {
    rng: ThreadRng,
    heartbeat_timeout: Option<time::Instant>,
    election_timeout: Option<time::Instant>,
}

impl Timing {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        let start_time = time::Instant::now();
        let init_election_timeout = gen_election_timeout(&mut rng);
        Timing {
            rng,
            heartbeat_timeout: None,
            election_timeout: Some(start_time + init_election_timeout),
        }
    }
    fn reset_heartbeat_timeout(&mut self) {
        if self.heartbeat_timeout.is_some() {
            let current_time = time::Instant::now();
            self.heartbeat_timeout = Some(current_time + gen_heartbeat_timeout(&mut self.rng));
        }
    }
    fn reset_election_timeout(&mut self) {
        if self.election_timeout.is_some() {
            let current_time = time::Instant::now();
            self.election_timeout = Some(current_time + gen_election_timeout(&mut self.rng));
        }
    }
    fn reset_when_become(&mut self, state: RoleState) {
        let current_time = time::Instant::now();
        match state {
            RoleState::Follower => {
                self.election_timeout = Some(current_time + gen_election_timeout(&mut self.rng));
                self.heartbeat_timeout = None;
            }
            RoleState::Candidate => {
                self.election_timeout = Some(current_time + gen_election_timeout(&mut self.rng));
                self.heartbeat_timeout = Some(current_time + gen_heartbeat_timeout(&mut self.rng));
            }
            RoleState::Leader => {
                self.election_timeout = None;
                self.heartbeat_timeout = Some(current_time + gen_heartbeat_timeout(&mut self.rng));
            }
        }
    }
    fn next_timeout(&mut self) -> (time::Instant, TimeoutType) {
        match (self.heartbeat_timeout, self.election_timeout) {
            (Some(heartbeat), Some(election)) => {
                if election > heartbeat {
                    (heartbeat, TimeoutType::Heartbeat)
                } else {
                    (election, TimeoutType::Election)
                }
            }
            (None, Some(election)) => (election, TimeoutType::Election),
            (Some(heartbeat), None) => (heartbeat, TimeoutType::Heartbeat),
            (None, None) => panic!("I have nothing to wait for"),
        }
    }
}

const ELECTION_TIMEOUT_MIN: u64 = 300;
const ELECTION_TIMEOUT_MAX: u64 = 600;
const HEARTBEAT_TIMEOUT: u64 = 100;

fn gen_election_timeout(rng: &mut ThreadRng) -> time::Duration {
    time::Duration::from_millis(rng.gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
}

fn gen_heartbeat_timeout(rng: &mut ThreadRng) -> time::Duration {
    time::Duration::from_millis(HEARTBEAT_TIMEOUT)
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // state: Arc<State>,
    apply_ch: mpsc::UnboundedSender<ApplyMsg>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    current_term: u64,
    log: Vec<Log>,

    commit_index: u64,
    last_applied: u64,

    // if leader is self, we are leader
    leader_id: Option<u64>,
    // if voted for self, we are candidate
    voted_for: Option<u64>,
    // elsewise, we are follower

    // special states for candidate
    votes_granted: HashSet<u64>,
    votes_not_respond: HashSet<u64>,
    // special states for leader
    next_index: Vec<u64>,
    match_index: Vec<u64>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: mpsc::UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let peers_count = peers.len();
        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            // state: Arc::default(),
            apply_ch,
            current_term: 0,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            voted_for: None,
            leader_id: None,
            votes_granted: HashSet::new(),
            votes_not_respond: HashSet::new(),
            next_index: vec![0; peers_count],
            match_index: vec![0; peers_count],
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// a helper used to get the current role state
    fn role_state(&self) -> RoleState {
        if self.leader_id == Some(self.me as u64) {
            return RoleState::Leader;
        }
        if self.voted_for == Some(self.me as u64) {
            return RoleState::Candidate;
        }
        RoleState::Follower
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns OK(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
        sender: &mpsc::UnboundedSender<Command>,
    ) {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();

        trace!("Raft #{:?}: Send request vote {:?}", self.me, args);
        let sender_clone = sender.clone();
        peer.spawn(
            peer.request_vote(&args)
                .map_err(|err| ())
                .and_then(move |res| {
                    push_inbound(&sender_clone, Incoming::Vote(server as u64, res));
                    Ok(())
                }),
        );
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
        sender: &mpsc::UnboundedSender<Command>,
    ) {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        debug!("Raft #{:?}: Send append entries {:?}", self.me, args);
        let sender_clone = sender.clone();
        peer.spawn(
            peer.append_entries(&args)
                .map_err(|err| ())
                .and_then(move |res| {
                    push_inbound(&sender_clone, Incoming::Feedback(server as u64, res));
                    Ok(())
                }),
        );
    }

    fn state(&self) -> State {
        State {
            term: self.current_term,
            is_leader: self.role_state() == RoleState::Leader,
        }
    }

    fn peers_count(&self) -> u64 {
        self.peers.len() as u64
    }

    fn log_length(&self) -> u64 {
        self.log.len() as u64
    }

    fn reset(&mut self, role_state: RoleState) {
        match role_state {
            RoleState::Follower => {
                self.leader_id = None;
                self.voted_for = None;
            }
            RoleState::Candidate => {
                self.leader_id = None;
                self.voted_for = Some(self.me as u64);
                self.votes_granted.clear();
                self.votes_granted.insert(self.me as u64);
                self.votes_not_respond.clear();
                for id in 0..self.peers_count() {
                    if id != self.me as u64 {
                        self.votes_not_respond.insert(id as u64);
                    }
                }
            }
            RoleState::Leader => {
                self.leader_id = Some(self.me as u64);
                for id in 0..self.peers_count() as usize {
                    self.next_index[id] = self.log_length() + 1;
                    self.match_index[id] = if id != self.me { 0 } else { self.log_length() }
                }
            }
        }
    }

    fn update_vote(&mut self, peer: u64, granted: bool) {
        self.votes_not_respond.remove(&peer);
        if granted {
            self.votes_granted.insert(peer);
        }
    }

    fn vote(&mut self, id: u64, term: u64, last_log_index: u64, last_log_term: u64) -> bool {
        let vote = if let Some(voted_id) = self.voted_for {
            id == voted_id
        } else if self.current_term <= term {
            let (current_last_log_index, current_last_log_term) = self.last_log_info();
            debug!("Raft #{:?}: Check vote for {:?}, Self: index {:?} in term {:?}, Candidate: index {:?} in term {:?}", self.me, id,current_last_log_index,current_last_log_term,last_log_index,last_log_term);
            if current_last_log_term != last_log_term {
                current_last_log_term < last_log_term
            } else {
                current_last_log_index <= last_log_index
            }
        } else {
            false
        };
        if self.voted_for.is_none() && vote {
            info!("Raft #{:?}: Vote for {:?}", self.me, id);
            self.voted_for = Some(id)
        }
        vote
    }

    fn check_vote(&self) -> bool {
        self.votes_granted.len() as u64 * 2 > self.peers_count()
    }

    fn check_leader(&mut self, term: u64, leader: u64) {
        if term >= self.current_term {
            self.leader_id = Some(leader)
        }
    }

    fn add_log(&mut self, content: Vec<u8>) {
        self.log.push(Log {
            term: self.current_term,
            command: content,
        });
        self.next_index[self.me] += 1;
        self.match_index[self.me] += 1;
    }

    fn last_log_info(&mut self) -> (u64, u64) {
        let last_log = self.log.last();
        if let Some(log) = last_log {
            (self.log.len() as u64, log.term)
        } else {
            (0, 0)
        }
    }

    fn gen_heartbeat(&self, id: u64) -> (u64, u64, Vec<Log>) {
        let next_index = self.next_index[id as usize] as usize;
        if next_index < 1 {
            return (0, 0, vec![]);
        }
        let (prev_log_index, prev_log_term) = if next_index >= 2 {
            let prev_log = &self.log[next_index - 2];
            (self.next_index[id as usize] - 1, prev_log.term)
        } else {
            (0, 0)
        };
        // to be simple we only send one entry one time
        let entries: Vec<Log> = self
            .log
            .get(next_index - 1)
            .into_iter()
            .map(Clone::clone)
            .collect();
        (prev_log_index, prev_log_term, entries)
    }

    fn check_entries_valid(&self, term: u64, prev_log_index: u64, prev_log_term: u64) -> bool {
        if term < self.current_term {
            false
        } else if prev_log_index < 1 {
            true
        } else {
            let term = self
                .log
                .get(prev_log_index as usize - 1)
                .map(|log| log.term);
            term == Some(prev_log_term)
        }
    }

    fn truncate_log(&mut self, new_length: usize) {
        for (id, log) in self.log.split_off(new_length).into_iter().enumerate() {
            self.drop_entry(log.command, (id + new_length + 1) as u64);
        }
    }

    fn apply_log(&mut self, prev_log_index: u64, entries: Vec<Log>, leader_commit: u64) {
        let mut entries = entries;
        self.truncate_log(prev_log_index as usize);
        self.log.append(&mut entries);
        if leader_commit > self.commit_index {
            self.update_commit_index(std::cmp::min(leader_commit, self.log_length()));
        }
    }

    fn append_success(&mut self, id: u64) {
        let id = id as usize;
        let entries_count = if self.log.get(self.next_index[id] as usize - 1).is_some() {
            1
        } else {
            0
        };
        self.next_index[id] += entries_count;
        self.match_index[id] = self.next_index[id] - 1;
        if entries_count > 0 {
            self.check_commit();
        }
    }

    fn append_failed(&mut self, id: u64) {
        let id = id as usize;
        if self.next_index[id] > 1 {
            self.next_index[id] -= 1;
        }
    }

    fn check_commit(&mut self) {
        // a not that naive but not best implemention
        let mut match_index_sorted = self.match_index.clone();
        match_index_sorted.sort_unstable();
        debug!("Raft #{:?}: Match {:?}", self.me, match_index_sorted);

        let half_of_server = ((self.peers_count() - 1) / 2) as usize;
        let more_than_half_matched = match_index_sorted[half_of_server];
        if more_than_half_matched > 0
            && self.log[more_than_half_matched as usize - 1].term == self.current_term
        {
            self.update_commit_index(more_than_half_matched)
        }
    }
    fn update_commit_index(&mut self, index: u64) {
        if self.commit_index < index {
            info!("Raft #{:?}: Commited {:?}", self.me, index);
            for id in self.commit_index..index {
                self.commit_entry(self.log[id as usize].command.clone(), id + 1)
            }
            self.commit_index = index
        }
    }

    fn drop_entry(&mut self, command: Vec<u8>, index: u64) {
        self.apply_ch
            .unbounded_send(ApplyMsg {
                command_valid: false,
                command,
                command_index: index,
            })
            .unwrap_or_default();
    }

    fn commit_entry(&mut self, command: Vec<u8>, index: u64) {
        debug!("Raft #{:?}: ApplyMsg {:?} at {:?}", self.me, command, index);
        self.apply_ch
            .unbounded_send(ApplyMsg {
                command_valid: true,
                command,
                command_index: index,
            })
            .unwrap_or_default();
    }
}

struct RaftStore {
    raft: Raft,
    state: Arc<Mutex<DetailedState>>,
    sender: mpsc::UnboundedSender<Command>,
    timing: Timing,
}

impl RaftStore {
    fn me(&self) -> u64 {
        self.raft.me as u64
    }
    fn term(&self) -> u64 {
        self.raft.current_term
    }
    fn set_term(&mut self, term: u64) {
        self.raft.current_term = term
    }
    fn role_state(&self) -> RoleState {
        self.raft.role_state()
    }
    fn request_vote(&mut self) {
        debug!("Raft #{:?}: Request vote heartbeat", self.me());
        let term = self.term();
        let candidate_id = self.me();
        let (last_log_index, last_log_term) = self.raft.last_log_info();
        for id in &self.raft.votes_not_respond {
            self.raft.send_request_vote(
                *id as usize,
                &RequestVoteArgs {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                },
                &self.sender,
            )
        }
    }

    fn append_entries(&mut self) {
        debug!("Raft #{:?}: Append entries heartbeat", self.me());
        let term = self.term();
        let leader_id = self.me();
        let leader_commit = self.raft.commit_index;
        for id in 0..self.raft.peers_count() {
            if id != self.me() {
                let (prev_log_index, prev_log_term, entries) = self.raft.gen_heartbeat(id);
                self.raft.send_append_entries(
                    id as usize,
                    &AppendEntriesArgs {
                        term,
                        leader_id,
                        leader_commit,
                        prev_log_index,
                        prev_log_term,
                        entries,
                    },
                    &self.sender,
                )
            }
        }
    }

    fn start_election(&mut self) {
        info!("Raft #{:?}: Start election, become candidate", self.me());
        //go to a higher term
        self.set_term(self.term() + 1);
        //init vote states
        self.raft.reset(RoleState::Candidate);
        //update timers
        self.timing.reset_when_become(RoleState::Candidate);
        //send vote request
        self.request_vote();
    }

    fn generic_request_handler(&mut self, term: u64) {
        if term > self.term() {
            info!("Raft #{:?}: Found higher term, become follower", self.me());
            //update term
            self.set_term(term);
            //convert to follower
            self.raft.reset(RoleState::Follower);
            //reset timing
            self.timing.reset_when_become(RoleState::Follower)
        } else {
            self.timing.reset_election_timeout()
        }
    }

    fn become_leader(&mut self) {
        info!("Raft #{:?}: Vote granted, become leader", self.me());
        // set leader
        self.raft.reset(RoleState::Leader);
        // reset timer
        self.timing.reset_when_become(RoleState::Leader);
        // send heartbeat
        self.append_entries()
    }

    fn process_request_vote(
        &mut self,
        args: RequestVoteArgs,
        sender: oneshot::Sender<RequestVoteReply>,
    ) {
        self.generic_request_handler(args.term);
        let vote = self.raft.vote(
            args.candidate_id,
            args.term,
            args.last_log_index,
            args.last_log_term,
        );
        sender
            .send(RequestVoteReply {
                term: self.term(),
                vote_granted: vote,
            })
            .unwrap_or_default();
    }

    fn process_append_entries(
        &mut self,
        args: AppendEntriesArgs,
        sender: oneshot::Sender<AppendEntriesReply>,
    ) {
        self.generic_request_handler(args.term);
        if self.role_state() == RoleState::Candidate {
            self.raft.reset(RoleState::Follower);
        }
        self.raft.check_leader(args.term, args.leader_id);
        let success =
            self.raft
                .check_entries_valid(args.term, args.prev_log_index, args.prev_log_term);
        if success {
            if !args.entries.is_empty() {
                info!(
                    "Raft #{:?}: Apply entries from {:?} in range {:?} to {:?}",
                    self.me(),
                    args.leader_id,
                    args.prev_log_index + 1,
                    args.prev_log_index + args.entries.len() as u64,
                );
            }
            self.raft
                .apply_log(args.prev_log_index, args.entries, args.leader_commit)
        }
        sender
            .send(AppendEntriesReply {
                term: self.term(),
                success,
            })
            .unwrap_or_default();
    }

    fn process_vote(&mut self, id: u64, reply: RequestVoteReply) {
        self.generic_request_handler(reply.term);
        if self.role_state() == RoleState::Candidate {
            self.raft.update_vote(id, reply.vote_granted);
            if self.raft.check_vote() {
                self.become_leader()
            }
        }
    }

    fn process_feedback(&mut self, id: u64, reply: AppendEntriesReply) {
        self.generic_request_handler(reply.term);
        if reply.success {
            self.raft.append_success(id)
        } else {
            self.raft.append_failed(id)
        }
    }

    fn process_log(&mut self, content: Vec<u8>) {
        if self.role_state() == RoleState::Leader {
            let term = self.term();
            let index = self.raft.log_length() + 1;
            info!(
                "Raft #{:?}: Add log in term {:?} at index {:?}",
                self.me(),
                term,
                index
            );
            self.raft.add_log(content)
        } else {
            self.raft.drop_entry(content, self.raft.log_length() + 1)
        }
    }

    fn process_task(&mut self, task: Task) {
        match (self.role_state(), task) {
            (RoleState::Follower, Task::Timeout(TimeoutType::Heartbeat)) => {
                panic!("Follower should not has heartbeat timeout")
            }
            (RoleState::Follower, Task::Timeout(TimeoutType::Election)) => self.start_election(),
            (RoleState::Candidate, Task::Timeout(TimeoutType::Heartbeat)) => {
                self.timing.reset_heartbeat_timeout();
                self.request_vote()
            }
            (RoleState::Candidate, Task::Timeout(TimeoutType::Election)) => self.start_election(),
            (RoleState::Leader, Task::Timeout(TimeoutType::Heartbeat)) => {
                self.timing.reset_heartbeat_timeout();
                self.append_entries()
            }
            (RoleState::Leader, Task::Timeout(TimeoutType::Election)) => {
                panic!("Leader should not has heartbeat timeout")
            }
            (_, Task::Packet(Incoming::RequestVote(arg, sender))) => {
                self.process_request_vote(arg, sender)
            }
            (_, Task::Packet(Incoming::AppendEntries(arg, sender))) => {
                self.process_append_entries(arg, sender)
            }
            (_, Task::Packet(Incoming::Vote(id, reply))) => self.process_vote(id, reply),
            (_, Task::Packet(Incoming::Feedback(id, reply))) => self.process_feedback(id, reply),
            (_, Task::Packet(Incoming::Log(content))) => self.process_log(content),
        }
    }
    fn update_state(&mut self) {
        let mut detailed_state = self.state.lock().unwrap();
        detailed_state.state = self.raft.state();
        if self.role_state() != RoleState::Leader {
            detailed_state.expected_log_length = self.raft.log_length()
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    sender: mpsc::UnboundedSender<Command>,
    state: Arc<Mutex<DetailedState>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (sender, receiver) = mpsc::unbounded::<Command>();
        let sender_clone = sender.clone();
        let state = Arc::new(Mutex::new(DetailedState {
            state: State {
                term: 0,
                is_leader: false,
            },
            expected_log_length: 0,
        }));
        let state_clone = state.clone();
        thread::spawn(move || raft_thread(raft, state_clone, receiver, sender_clone));
        Node { sender, state }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns false. otherwise start the
    /// agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first return value is the index that the command will appear at
    /// if it's ever committed. the second return value is the current
    /// term. the third return value is true if this server believes it is
    /// the leader.
    /// This method must return quickly.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let mut detailed_state = self.state.lock().unwrap();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if detailed_state.state.is_leader {
            //index here start from 1
            detailed_state.expected_log_length += 1;
            let index = detailed_state.expected_log_length;
            push_inbound(&self.sender, Incoming::Log(buf));
            Ok((index, detailed_state.state.term))
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.get_state().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.state.lock().unwrap().state.clone()
    }

    /// the tester calls kill() when a Raft instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.sender
            .unbounded_send(Command::Kill)
            .unwrap_or_default();
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        let (sender, receiver) = oneshot::channel::<RequestVoteReply>();
        push_inbound(&self.sender, Incoming::RequestVote(args, sender));
        Box::new(receiver.map_err(labrpc::Error::Recv))
    }
    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // Your code here (2A, 2B).
        let (sender, receiver) = oneshot::channel::<AppendEntriesReply>();
        push_inbound(&self.sender, Incoming::AppendEntries(args, sender));
        Box::new(receiver.map_err(labrpc::Error::Recv))
    }
}

fn raft_thread(
    raft: Raft,
    state: Arc<Mutex<DetailedState>>,
    receiver: mpsc::UnboundedReceiver<Command>,
    sender: mpsc::UnboundedSender<Command>,
) {
    let mut raft_store = RaftStore {
        raft,
        state,
        sender,
        timing: Timing::new(),
    };
    let mut receiver_future = receiver.into_future();
    info!("Raft #{:?}: Node created", raft_store.me());
    loop {
        // select next timeout
        let (timeout_instant, timeout_type) = raft_store.timing.next_timeout();
        let timeout = Delay::new_at(timeout_instant);

        // wait for message+timer
        let wait_result = receiver_future.select2(timeout).wait();
        let (task, new_receiver_future) = match wait_result {
            Ok(Either::A(((Some(Command::Inbound(message)), stream), _))) => {
                (Task::Packet(message), stream.into_future())
            }
            Ok(Either::A(((Some(Command::Kill), stream), _))) => {
                // node explicitly killed
                info!("Raft #{:?}: Node destroyed", raft_store.me());
                return;
            }
            Ok(Either::A(((None, stream), _))) => {
                // no more incoming message to execute, the sender is dropped and the node is destroyed
                info!("Raft #{:?}: Node destroyed", raft_store.me());
                return;
            }
            Err(Either::A(((error, stream), _))) => panic!("My channel gives me an error!"),
            Ok(Either::B((_, stream_future))) => (Task::Timeout(timeout_type), stream_future),
            Err(Either::B((error, stream_future))) => panic!("My timer gives me an error!"),
        };
        trace!("Raft #{:?}: Get task {:?}", raft_store.me(), task);
        receiver_future = new_receiver_future;

        // process message/timeout
        raft_store.process_task(task);

        //update state
        raft_store.update_state()
    }
}
