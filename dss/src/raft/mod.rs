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
    fn reset_when_becomes(&mut self, state: RoleState) {
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
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    current_term: u64,

    // if leader is self, we are leader
    leader_id: Option<u64>,
    // if voted for self, we are candidate
    voted_for: Option<u64>,
    // elsewise, we are follower

    // special states for candidate
    votes_granted: HashSet<u64>,
    votes_not_respond: HashSet<u64>,
    // special states for leader
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

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            // state: Arc::default(),
            current_term: 0,
            voted_for: None,
            leader_id: None,
            votes_granted: HashSet::new(),
            votes_not_respond: HashSet::new(),
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

        debug!("Raft #{:?}: Send request vote {:?}", self.me, args);
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

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
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
            }
        }
    }

    fn update_vote(&mut self, peer: u64, granted: bool) {
        self.votes_not_respond.remove(&peer);
        if granted {
            self.votes_granted.insert(peer);
        }
    }

    fn vote(&mut self, id: u64, term: u64) -> bool {
        let vote = if let Some(voted_id) = self.voted_for {
            id == voted_id
        } else {
            // TODO: log related vote
            self.current_term <= term
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
}

struct RaftStore {
    raft: Raft,
    state: Arc<Mutex<State>>,
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
        for id in &self.raft.votes_not_respond {
            self.raft.send_request_vote(
                *id as usize,
                &RequestVoteArgs {
                    term: self.term(),
                    candidate_id: self.me(),
                },
                &self.sender,
            )
        }
    }

    fn append_entries(&mut self) {
        for id in 0..self.raft.peers_count() {
            if id != self.me() {
                self.raft.send_append_entries(
                    id as usize,
                    &AppendEntriesArgs {
                        term: self.term(),
                        leader_id: self.me(),
                    },
                    &self.sender,
                )
            }
        }
    }

    fn start_election(&mut self) {
        info!("Raft #{:?}: Start election, becomes candidate", self.me());
        //go to a higher term
        self.set_term(self.term() + 1);
        //init vote states
        self.raft.reset(RoleState::Candidate);
        //update timers
        self.timing.reset_when_becomes(RoleState::Candidate);
        //send vote request
        self.request_vote();
    }

    fn generic_request_handler(&mut self, term: u64) {
        if term > self.term() {
            info!("Raft #{:?}: Found higher term, becomes follower", self.me());
            //update term
            self.set_term(term);
            //convert to follower
            self.raft.reset(RoleState::Follower);
            //reset timing
            self.timing.reset_when_becomes(RoleState::Follower)
        } else {
            self.timing.reset_election_timeout()
        }
    }

    fn becomes_leader(&mut self) {
        info!("Raft #{:?}: Vote granted, becomes leader", self.me());
        // set leader
        self.raft.reset(RoleState::Leader);
        // reset timer
        self.timing.reset_when_becomes(RoleState::Leader);
        // send heartbeat
        self.append_entries()
    }

    fn process_request_vote(
        &mut self,
        args: RequestVoteArgs,
        sender: oneshot::Sender<RequestVoteReply>,
    ) {
        self.generic_request_handler(args.term);
        let vote = self.raft.vote(args.candidate_id, args.term);
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
        sender
            .send(AppendEntriesReply { term: self.term() })
            .unwrap_or_default();
    }

    fn process_vote(&mut self, id: u64, reply: RequestVoteReply) {
        self.generic_request_handler(reply.term);
        if self.role_state() == RoleState::Candidate {
            self.raft.update_vote(id, reply.vote_granted);
            if self.raft.check_vote() {
                self.becomes_leader()
            }
        }
    }

    fn process_feedback(&mut self, id: u64, reply: AppendEntriesReply) {
        self.generic_request_handler(reply.term);
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
        }
    }
    fn update_state(&mut self) {
        *self.state.lock().unwrap() = self.raft.state();
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
    state: Arc<Mutex<State>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (sender, receiver) = mpsc::unbounded::<Command>();
        let sender_clone = sender.clone();
        let state = Arc::new(Mutex::new(State {
            term: 0,
            is_leader: false,
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
        unimplemented!()
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
        (*self.state.lock().unwrap()).clone()
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
    state: Arc<Mutex<State>>,
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
        debug!("Raft #{:?}: Get task {:?}", raft_store.me(), task);
        receiver_future = new_receiver_future;

        // process message/timeout
        raft_store.process_task(task);

        //update state
        raft_store.update_state()
    }
}
