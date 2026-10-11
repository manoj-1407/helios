package raft

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds the timing knobs every RaftNode needs. Production defaults
// (DefaultConfig) follow the values the Raft paper itself uses as a
// reference point (150-300ms election timeout, heartbeats well under
// that). Tests use a much faster Config so the whole suite runs in
// seconds instead of minutes -- the algorithm doesn't care what units
// time is measured in, only that heartbeat << election timeout holds.
type Config struct {
	ElectionTimeoutBase   time.Duration
	ElectionTimeoutSpread time.Duration
	HeartbeatInterval     time.Duration
	RPCTimeout            time.Duration
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutBase:   150 * time.Millisecond,
		ElectionTimeoutSpread: 150 * time.Millisecond,
		HeartbeatInterval:     50 * time.Millisecond,
		RPCTimeout:            100 * time.Millisecond,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.ElectionTimeoutBase == 0 {
		c.ElectionTimeoutBase = d.ElectionTimeoutBase
	}
	if c.ElectionTimeoutSpread == 0 {
		c.ElectionTimeoutSpread = d.ElectionTimeoutSpread
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = d.RPCTimeout
	}
	return c
}

type RaftMetrics struct {
	mu                sync.Mutex
	ReplicationLagNs  map[string]int64 // per-follower, nanoseconds from append to acked
	HeartbeatsSent    atomic.Int64
	EntriesReplicated atomic.Int64
	LeaderChanges     atomic.Int64
	CompactionEvents  atomic.Int64 // wired up once store/bridge.go exists (Week 3)
}

func newRaftMetrics() *RaftMetrics {
	return &RaftMetrics{ReplicationLagNs: make(map[string]int64)}
}

func (m *RaftMetrics) recordReplicationLag(peer string, lag time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReplicationLagNs[peer] = lag.Nanoseconds()
}

type RaftNode struct {
	mu sync.Mutex

	id    string
	peers []string
	cfg   Config

	state     NodeState
	persisted PersistentState
	volatile  VolatileState
	leader    *LeaderVolatileState // nil when not leader

	log       *RaftLog
	transport Transport
	sm        StateMachine

	dataDir string

	// lastHeartbeatSeen is read by HandlePreVote (and used internally by
	// the election-timeout path) to decide "have we heard from a valid
	// leader recently enough that we wouldn't be timing out ourselves."
	lastHeartbeatSeen time.Time

	electionTimer *time.Timer
	stepDownC     chan struct{} // recreated fresh every time this node becomes leader; closed on step-down to stop the heartbeat loop
	commitC       chan uint64   // non-blocking signal to the applier goroutine that commitIndex advanced
	applyWg       sync.WaitGroup
	stopped       bool
	stopC         chan struct{}

	metrics    *RaftMetrics
	RepMetrics *ReplicationMetrics // exported so store/bridge can read pressure level
}

// NewRaftNode constructs a node and loads whatever persistent state and
// log already exist at dataDir -- a brand new node and a node recovering
// from a crash go through the identical path here. It does NOT start any
// timers or goroutines; call Start() once the node is registered on its
// transport, so it can't possibly try to contact a peer that doesn't know
// about it yet.
func NewRaftNode(id string, peers []string, dataDir string, transport Transport, sm StateMachine, cfg Config) (*RaftNode, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating data dir for node %s: %w", id, err)
	}
	persisted, err := LoadPersistentState(dataDir)
	if err != nil {
		return nil, fmt.Errorf("loading persistent state for node %s: %w", id, err)
	}
	log, err := NewRaftLog(dataDir + "/log.bin")
	if err != nil {
		return nil, fmt.Errorf("opening raft log for node %s: %w", id, err)
	}

	return &RaftNode{
		id:        id,
		peers:     peers,
		cfg:       cfg.withDefaults(),
		state:     Follower,
		persisted: persisted,
		log:       log,
		transport: transport,
		sm:        sm,
		dataDir:   dataDir,
		commitC:    make(chan uint64, 1),
		stopC:      make(chan struct{}),
		metrics:    newRaftMetrics(),
		RepMetrics: NewReplicationMetrics(),
	}, nil
}

func (r *RaftNode) ID() string { return r.id }

func (r *RaftNode) State() NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

func (r *RaftNode) Term() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.persisted.CurrentTerm
}

func (r *RaftNode) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.volatile.CommitIndex
}

// Start kicks off the election timer and the applier goroutine. Safe to
// call exactly once per node lifetime (chaos.RestartNode constructs a
// brand new RaftNode rather than reusing one, which is also more honest
// about what a real process restart looks like).
func (r *RaftNode) Start() {
	r.mu.Lock()
	r.lastHeartbeatSeen = time.Now() // don't immediately time out the instant we start
	r.mu.Unlock()
	r.resetElectionTimer()
	r.applyWg.Add(1)
	go r.applyLoop()
}

// Stop halts the election timer, signals the applier goroutine to exit,
// and closes the log file. Does NOT delete any on-disk state -- a stopped
// node and a crashed node should be indistinguishable to whoever restarts
// them later.
func (r *RaftNode) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	if r.state == Leader && r.stepDownC != nil {
		close(r.stepDownC)
		r.stepDownC = nil
	}
	close(r.stopC)
	r.mu.Unlock()

	r.applyWg.Wait()
	r.log.Close()
}

// resetElectionTimerLocked assumes the caller already holds r.mu --
// used from inside HandleAppendEntries/HandleRequestVote, which hold the
// lock for their entire body and would deadlock against the
// lock-acquiring resetElectionTimer() below.
func (r *RaftNode) resetElectionTimerLocked() {
	if r.stopped {
		return
	}
	timeout := jitterDuration(r.cfg.ElectionTimeoutBase, r.cfg.ElectionTimeoutSpread)
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	r.electionTimer = time.AfterFunc(timeout, r.onElectionTimeout)
}

// resetElectionTimer acquires r.mu itself -- only call this from a path
// that does NOT already hold the lock (onElectionTimeout's own
// rescheduling, runElection's "didn't reach quorum" tail). Earlier this
// also got called as `go r.resetElectionTimer()` from inside the heartbeat
// handler to dodge the self-deadlock; that was a real bug, not just an
// odd style choice -- spawning a brand new goroutine on every single
// incoming heartbeat (every ~15ms in tests, every ~50ms in production)
// gives no ordering guarantee at all between resets, so under any
// goroutine-scheduling pressure a STALE reset from several heartbeats ago
// could run after a fresher one and arm a timer that's about to fire
// almost immediately -- which is exactly what caused TestLeaderSendsHeartbeats
// to intermittently see "healthy followers" spuriously time out and
// re-elect, even with a generous heartbeat:timeout margin. The fix is to
// never need the goroutine at all: resetElectionTimerLocked() runs
// synchronously inside the same locked section that's already handling
// the heartbeat, so there's exactly one reset per heartbeat, in the same
// order the heartbeats themselves arrived.
func (r *RaftNode) resetElectionTimer() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetElectionTimerLocked()
}

func (r *RaftNode) onElectionTimeout() {
	r.mu.Lock()
	if r.stopped || r.state == Leader {
		r.mu.Unlock()
		return // leaders don't run elections against themselves
	}
	r.mu.Unlock()
	r.runElection()
}

// runElection is the full election flow: pre-vote first, then (only if
// pre-vote got a quorum) the real term-incrementing election.
//
// baseTerm is captured BEFORE the (blocking, network-RPC-bound) pre-vote
// round and re-checked after it returns. This matters: pre-vote can take
// up to an RPC timeout to resolve, and this node's own term can
// legitimately change while it's waiting -- most commonly, by granting a
// REAL vote to a different candidate that got there first. Without this
// check, a stale pre-vote result would still go on to unconditionally
// increment CurrentTerm and overwrite VotedFor, silently discarding a
// vote this node had already legitimately cast for someone else. That
// was a real bug here, not a hypothetical: it surfaced under exactly the
// 3-node startup race where two nodes' election timers fire close
// together, and is exactly the kind of thing pre-vote being a separate,
// blocking phase makes possible if the term it was computed against
// isn't re-validated afterward.
func (r *RaftNode) runElection() {
	r.mu.Lock()
	baseTerm := r.persisted.CurrentTerm
	r.mu.Unlock()

	if !r.runPreVote() {
		r.resetElectionTimer() // no quorum interest yet -- back off and let the timer try again later
		return
	}

	r.mu.Lock()
	if r.stopped || r.state == Leader || r.persisted.CurrentTerm != baseTerm {
		// Stale: something changed our term (or we stopped, or somehow
		// already became leader) while pre-vote was in flight. This
		// pre-vote result no longer reflects the world we'd be acting in
		// -- abandon it rather than barrel ahead. Whatever changed our
		// state already reset our election timer if a future election is
		// still warranted (becomeFollowerLocked does not reset it itself,
		// but the RequestVote/AppendEntries handler that called it does,
		// right after granting/accepting).
		r.mu.Unlock()
		return
	}
	r.persisted.CurrentTerm++
	r.persisted.VotedFor = r.id
	term := r.persisted.CurrentTerm
	r.state = Candidate
	lastIndex, lastTerm := r.log.LastIndexAndTerm()
	peers := append([]string(nil), r.peers...)
	if err := SavePersistentState(r.dataDir, r.persisted); err != nil {
		// Critical invariant: currentTerm/votedFor must be durable before
		// any RPC goes out. If we can't even persist, we cannot safely
		// proceed -- revert the in-memory bump and bail rather than run
		// an election whose outcome we couldn't honor on a crash.
		r.persisted.CurrentTerm--
		r.persisted.VotedFor = ""
		r.state = Follower
		r.mu.Unlock()
		r.resetElectionTimer()
		return
	}
	r.mu.Unlock()

	quorumNeeded := len(peers)/2 + 1
	granted := 1 // voted for self

	if len(peers) == 0 {
		r.becomeLeader(term)
		return
	}

	type voteResult struct {
		granted  bool
		peerTerm uint64
	}
	resultsC := make(chan voteResult, len(peers))

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
			defer cancel()
			resp, err := r.transport.SendRequestVote(ctx, peer, RequestVoteRequest{
				Term:         term,
				CandidateID:  r.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				resultsC <- voteResult{granted: false}
				return
			}
			resultsC <- voteResult{granted: resp.VoteGranted, peerTerm: resp.Term}
		}()
	}

	deadline := time.After(r.cfg.RPCTimeout + 20*time.Millisecond)
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-resultsC:
			r.mu.Lock()
			stillRelevant := !r.stopped && r.state == Candidate && r.persisted.CurrentTerm == term
			if res.peerTerm > r.persisted.CurrentTerm {
				r.becomeFollowerLocked(res.peerTerm)
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
			if !stillRelevant {
				return
			}
			if res.granted {
				granted++
			}
			if granted >= quorumNeeded {
				r.becomeLeader(term)
				return
			}
		case <-deadline:
			i = len(peers) // stop waiting on stragglers
		}
	}

	// Didn't reach quorum within the timeout. Stay a candidate; the
	// election timer (still pending from before this round started) will
	// fire again and retry with a new term.
	r.mu.Lock()
	stillCandidate := !r.stopped && r.state == Candidate && r.persisted.CurrentTerm == term
	r.mu.Unlock()
	if stillCandidate {
		r.resetElectionTimer()
	}
}

func (r *RaftNode) becomeLeader(term uint64) {
	r.mu.Lock()
	if r.stopped || r.persisted.CurrentTerm != term || r.state != Candidate {
		r.mu.Unlock()
		return // stale -- something changed while votes were being collected
	}
	r.state = Leader
	lastIndex, _ := r.log.LastIndexAndTerm()
	next := make(map[string]uint64)
	match := make(map[string]uint64)
	for _, p := range r.peers {
		next[p] = lastIndex + 1
		match[p] = 0
	}
	r.leader = &LeaderVolatileState{NextIndex: next, MatchIndex: match}
	r.stepDownC = make(chan struct{})
	r.metrics.LeaderChanges.Add(1)
	stepDownC := r.stepDownC
	r.mu.Unlock()

	r.replicateToAllPeers() // immediately assert leadership with an empty AppendEntries round
	go r.heartbeatLoop(stepDownC)
}

// becomeFollowerLocked transitions to Follower, persists the new term,
// and always arms a fresh election timer. Caller MUST already hold
// r.mu -- this never acquires or releases the lock itself, which is what
// makes it safe to call from inside runElection's response-handling loop
// without a nested-lock bug.
//
// The timer reset is not optional and used to live only at specific call
// sites (HandleAppendEntries, the grant-path in HandleRequestVote) --
// that left two paths able to step down to Follower with NO active
// timer at all: a candidate that saw a higher term in a vote *response*
// and stepped down, and a node that bumped its term but then denied the
// vote because the candidate's log wasn't up to date. Either way, a
// Follower with no live timer can never detect a future leader failure
// on its own and is permanently dependent on hearing from someone else --
// a real liveness bug, found by tracing exactly this scenario. Resetting
// the timer unconditionally inside this one function, rather than at
// every call site, is what makes it impossible to miss a path again.
func (r *RaftNode) becomeFollowerLocked(term uint64) {
	wasLeader := r.state == Leader
	r.persisted.CurrentTerm = term
	r.persisted.VotedFor = ""
	r.state = Follower
	r.leader = nil
	SavePersistentState(r.dataDir, r.persisted) // best-effort here; the critical path (vote granting) persists explicitly before replying
	if wasLeader && r.stepDownC != nil {
		close(r.stepDownC)
		r.stepDownC = nil
	}
	r.resetElectionTimerLocked()
}

func (r *RaftNode) heartbeatLoop(stepDownC chan struct{}) {
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.replicateToAllPeers()
		case <-stepDownC:
			return
		case <-r.stopC:
			return
		}
	}
}

func (r *RaftNode) replicateToAllPeers() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	peers := append([]string(nil), r.peers...)
	r.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		go r.replicateToPeer(peer)
	}
}

// replicateToPeer sends one AppendEntries to a single peer with whatever
// entries it's missing (which is an empty slice -- a pure heartbeat -- the
// moment NextIndex[peer] catches up to the end of the log). This is the
// same function for "heartbeat" and "replicate new entries"; real Raft
// heartbeats ARE just empty AppendEntries, so unifying them here isn't a
// simplification, it's what the protocol actually does.
func (r *RaftNode) replicateToPeer(peer string) {
	r.mu.Lock()
	if r.state != Leader || r.leader == nil {
		r.mu.Unlock()
		return
	}
	term := r.persisted.CurrentTerm
	nextIndex := r.leader.NextIndex[peer]
	prevLogIndex := nextIndex - 1
	prevEntry, ok := r.log.Get(prevLogIndex)
	if !ok {
		// We no longer have prevLogIndex (it was compacted into a
		// snapshot) -- Week 3's InstallSnapshot path handles this; for
		// now, back off and let the next heartbeat try again rather than
		// send a request we know references a gone entry.
		r.mu.Unlock()
		return
	}
	lastIndex, _ := r.log.LastIndexAndTerm()
	entries := r.log.Slice(nextIndex, lastIndex+1)
	leaderCommit := r.volatile.CommitIndex
	leaderID := r.id
	r.mu.Unlock()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	defer cancel()
	resp, err := r.transport.SendAppendEntries(ctx, peer, AppendEntriesRequest{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevEntry.Index,
		PrevLogTerm:  prevEntry.Term,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	})
	r.metrics.HeartbeatsSent.Add(1)
	if err != nil {
		return // dropped/timed-out -- next heartbeat tick will retry
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.state != Leader || r.persisted.CurrentTerm != term || r.leader == nil {
		return // stale response for a round we're no longer in
	}
	if resp.Term > r.persisted.CurrentTerm {
		r.becomeFollowerLocked(resp.Term)
		return
	}

	if resp.Success {
		newMatch := prevEntry.Index + uint64(len(entries))
		r.leader.MatchIndex[peer] = newMatch
		r.leader.NextIndex[peer] = newMatch + 1
		if len(entries) > 0 {
			r.metrics.EntriesReplicated.Add(int64(len(entries)))
			r.metrics.recordReplicationLag(peer, time.Since(start))
		}
		// Update replication pressure metrics: lag = leaderLastIndex - matchIndex
		lastIndex, _ := r.log.LastIndexAndTerm()
		r.RepMetrics.UpdateLag(peer, lastIndex, newMatch)
		r.maybeAdvanceCommitIndexLocked()
		return
	}

	// Fast conflict resolution (Raft §5.3): jump NextIndex directly to
	// where the logs might actually agree instead of retrying one index
	// at a time.
	if resp.ConflictTerm == 0 {
		r.leader.NextIndex[peer] = resp.ConflictIndex
	} else {
		lastIdxForTerm := r.lastIndexForTermLocked(resp.ConflictTerm)
		if lastIdxForTerm > 0 {
			r.leader.NextIndex[peer] = lastIdxForTerm + 1
		} else {
			r.leader.NextIndex[peer] = resp.ConflictIndex
		}
	}
}

// lastIndexForTermLocked scans backward from the end of the log for the
// last entry with the given term. Caller must hold r.mu. A linear scan is
// fine here -- conflict resolution is already the slow, rare path
// compared to normal replication, and logs in this engine are bounded by
// snapshotting (once that's implemented) rather than growing without limit.
func (r *RaftNode) lastIndexForTermLocked(term uint64) uint64 {
	lastIndex, _ := r.log.LastIndexAndTerm()
	for i := lastIndex; i > 0; i-- {
		e, ok := r.log.Get(i)
		if !ok {
			break
		}
		if e.Term == term {
			return i
		}
		if e.Term < term {
			break
		}
	}
	return 0
}

// maybeAdvanceCommitIndexLocked checks whether any index N has been
// replicated to a majority (including the leader itself, which always
// "has" everything in its own log) AND belongs to the leader's current
// term -- the second condition is the part every Raft summary glosses
// over and is exactly what Figure 8 in the paper exists to explain: a
// leader can only safely commit by counting replicas for an entry from
// its OWN term. Committing an old-term entry just because a majority
// happens to have it can be undone by a future leader in a way that
// committing a current-term entry never can, because log matching
// guarantees every entry before a committed current-term entry is
// committed too.
func (r *RaftNode) maybeAdvanceCommitIndexLocked() {
	lastIndex, _ := r.log.LastIndexAndTerm()
	for n := lastIndex; n > r.volatile.CommitIndex; n-- {
		entry, ok := r.log.Get(n)
		if !ok || entry.Term != r.persisted.CurrentTerm {
			continue
		}
		count := 1 // leader itself
		for _, peer := range r.peers {
			if r.leader.MatchIndex[peer] >= n {
				count++
			}
		}
		if count >= len(r.peers)/2+1 {
			r.volatile.CommitIndex = n
			select {
			case r.commitC <- n:
			default: // applier is already going to pick up the latest value; no need to block or queue
			}
			return
		}
	}
}

// HandleRequestVote is the real (post-pre-vote) vote handler. Grants a
// vote only if the candidate's term is at least as current, this node
// hasn't already voted for someone else this term, and the candidate's
// log is at least as up to date.
func (r *RaftNode) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.persisted.CurrentTerm {
		return RequestVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: false}
	}
	if req.Term > r.persisted.CurrentTerm {
		r.becomeFollowerLocked(req.Term)
	}

	alreadyVotedForSomeoneElse := r.persisted.VotedFor != "" && r.persisted.VotedFor != req.CandidateID
	lastIndex, lastTerm := r.log.LastIndexAndTerm()
	candidateLogIsUpToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	if alreadyVotedForSomeoneElse || !candidateLogIsUpToDate {
		return RequestVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: false}
	}

	r.persisted.VotedFor = req.CandidateID
	if err := SavePersistentState(r.dataDir, r.persisted); err != nil {
		// Cannot durably record the vote -- must not grant it. Granting a
		// vote we can't remember on the next crash is exactly how a node
		// votes twice in the same term.
		r.persisted.VotedFor = ""
		return RequestVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: false}
	}
	r.lastHeartbeatSeen = time.Now() // granting a vote is also a reason to not immediately start our own election
	r.resetElectionTimerLocked()
	return RequestVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: true}
}

// HandleAppendEntries is both the heartbeat handler and the log
// replication handler -- see replicateToPeer's doc for why those are the
// same RPC.
func (r *RaftNode) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.persisted.CurrentTerm {
		return AppendEntriesResponse{Term: r.persisted.CurrentTerm, Success: false}
	}
	if req.Term > r.persisted.CurrentTerm || r.state == Candidate {
		r.becomeFollowerLocked(req.Term)
	}
	r.lastHeartbeatSeen = time.Now()
	r.resetElectionTimerLocked()

	prevEntry, ok := r.log.Get(req.PrevLogIndex)
	if !ok || prevEntry.Term != req.PrevLogTerm {
		conflictIndex, conflictTerm := r.findConflictLocked(req.PrevLogIndex)
		return AppendEntriesResponse{
			Term:          r.persisted.CurrentTerm,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		}
	}

	// Truncate on the first actual mismatch rather than blindly truncating
	// at PrevLogIndex+1 -- entries that already match don't need to be
	// rewritten, and re-truncating+re-appending identical data on every
	// heartbeat would mean an idle, fully-caught-up follower still pays a
	// disk fsync per heartbeat for nothing.
	appendFrom := 0
	for i, newEntry := range req.Entries {
		existing, ok := r.log.Get(newEntry.Index)
		if !ok {
			appendFrom = i
			break
		}
		if existing.Term != newEntry.Term {
			if err := r.log.TruncateFrom(newEntry.Index); err != nil {
				return AppendEntriesResponse{Term: r.persisted.CurrentTerm, Success: false}
			}
			appendFrom = i
			break
		}
		appendFrom = i + 1
	}

	if appendFrom < len(req.Entries) {
		if err := r.log.Append(req.Entries[appendFrom:]...); err != nil {
			return AppendEntriesResponse{Term: r.persisted.CurrentTerm, Success: false}
		}
	}

	lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
	if req.LeaderCommit > r.volatile.CommitIndex {
		newCommit := req.LeaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		if newCommit > r.volatile.CommitIndex {
			r.volatile.CommitIndex = newCommit
			select {
			case r.commitC <- newCommit:
			default:
			}
		}
	}

	return AppendEntriesResponse{Term: r.persisted.CurrentTerm, Success: true}
}

// findConflictLocked implements the follower side of fast conflict
// resolution: if we don't have PrevLogIndex at all, point the leader at
// the end of our log (ConflictTerm 0 signals "I'm just missing entries,
// not conflicting on a term"); if we have an entry there but with a
// different term, report that term and the first index we have for it,
// so the leader can skip past every entry it knows belongs to a term we
// don't share.
func (r *RaftNode) findConflictLocked(prevLogIndex uint64) (conflictIndex, conflictTerm uint64) {
	lastIndex, _ := r.log.LastIndexAndTerm()
	if prevLogIndex > lastIndex {
		return lastIndex + 1, 0
	}
	entry, ok := r.log.Get(prevLogIndex)
	if !ok {
		return lastIndex + 1, 0
	}
	term := entry.Term
	first := prevLogIndex
	for first > 1 {
		prev, ok := r.log.Get(first - 1)
		if !ok || prev.Term != term {
			break
		}
		first--
	}
	return first, term
}

// Propose is the client-facing write path: a leader appends the command
// to its own log (persisted before this returns), then immediately kicks
// off a replication round to every peer rather than waiting for the next
// heartbeat tick. Returns the index/term the entry was given so a caller
// can later check CommitIndex() to know when it's safe to consider the
// write durable cluster-wide. Returns ok=false if this node isn't the
// leader -- callers are expected to retry against whichever node the
// cluster reports as leader.
func (r *RaftNode) Propose(command []byte) (index uint64, term uint64, ok bool) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, 0, false
	}
	lastIndex, _ := r.log.LastIndexAndTerm()
	entry := LogEntry{Term: r.persisted.CurrentTerm, Index: lastIndex + 1, Command: command}
	if err := r.log.Append(entry); err != nil {
		r.mu.Unlock()
		return 0, 0, false
	}
	if len(r.peers) == 0 {
		// Single-node cluster: this node's own log IS the majority.
		r.volatile.CommitIndex = entry.Index
		select {
		case r.commitC <- entry.Index:
		default:
		}
	}
	idx, t := entry.Index, entry.Term
	r.mu.Unlock()

	r.replicateToAllPeers()
	return idx, t, true
}

// applyLoop watches for commitIndex advancing and applies every newly
// committed entry to the state machine, in order, exactly once. This is
// the one place LastApplied is mutated, which is what keeps "apply order
// matches commit order, no entry applied twice" simple to reason about --
// there's exactly one goroutine per node that's allowed to touch the
// state machine.
func (r *RaftNode) applyLoop() {
	defer r.applyWg.Done()
	for {
		select {
		case <-r.stopC:
			return
		case <-r.commitC:
			r.applyCommitted()
		}
	}
}

func (r *RaftNode) applyCommitted() {
	for {
		r.mu.Lock()
		if r.volatile.LastApplied >= r.volatile.CommitIndex {
			r.mu.Unlock()
			return
		}
		next := r.volatile.LastApplied + 1
		entry, ok := r.log.Get(next)
		r.mu.Unlock()
		if !ok {
			return // shouldn't happen if commitIndex bookkeeping is correct; bail rather than spin
		}
		if err := r.sm.Apply(entry); err != nil {
			// An Apply failure is a state machine bug or a transient
			// resource problem (e.g. the helios-kv bridge process is
			// down) -- either way, applying out of order would be worse
			// than stalling, so this node simply stops making progress on
			// applies until whatever's wrong is fixed and a future commit
			// notification retries from the same LastApplied.
			return
		}
		r.mu.Lock()
		r.volatile.LastApplied = next
		r.mu.Unlock()
	}
}
