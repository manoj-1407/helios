package raft

import (
	"context"
	"time"
)

// Why pre-vote exists: without it, a node that's been partitioned away
// for a while keeps incrementing its term every time its election timer
// fires with no leader in sight. When the partition heals, it sends
// RequestVote with that very high term. A real RequestVote handler has to
// accept any higher term as authoritative -- that's the rule that makes
// the rest of Raft work -- so the healthy leader is forced to step down
// even though the isolated node's log is stale and could never actually
// win the election. That's a real, disruptive, unnecessary leader change
// and replication pause, caused by nothing more than one node being cut
// off for a while.
//
// Pre-vote fixes this by asking the question first, without consequence:
// "if I were to start a real election, would you vote for me?" A peer can
// answer this honestly without changing CurrentTerm or VotedFor, because
// pre-vote isn't really voting -- it's a forecast. Only if a candidate
// gets a quorum of "yes" does it bother incrementing its term and running
// the real election that actually changes persistent state. An isolated
// node asking this question after healing gets a quorum of "no" (the
// healthy peers have a perfectly good leader and recent heartbeats), so
// it never even reaches the part that would disrupt anyone.
type PreVoteRequest struct {
	NextTerm     uint64 // the term the candidate WOULD use if it proceeds
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type PreVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// HandlePreVote grants a pre-vote if and only if:
//  1. This node hasn't heard from a valid leader recently -- i.e. it would
//     be willing to start its own election under the normal timeout rule.
//     A node with a healthy, recently-heard-from leader says no
//     regardless of how convincing the candidate's log is, because there
//     is nothing to fix and granting would only invite disruption.
//  2. The candidate's log is at least as up to date as this node's own
//     (same rule RequestVote uses: higher last-log-term wins outright,
//     equal term means longer log wins).
//
// It deliberately does NOT check or touch VotedFor -- that's the entire
// point of pre-vote being side-effect-free. Two different candidates can
// both get a "yes" pre-vote from the same peer in the same round; that's
// fine, because the real election afterward is where the one-vote-per-term
// rule actually applies.
func (r *RaftNode) HandlePreVote(req PreVoteRequest) PreVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Leader {
		// A functioning leader doesn't run an election timer at all --
		// "have I heard from a leader recently" is meaningless for it,
		// because it IS the leader. Without this check, lastHeartbeatSeen
		// (only ever set once, at Start(), for a node that wins an
		// election and never has to grant or receive another vote) goes
		// stale the moment enough wall-clock time passes, and the leader
		// would start granting pre-votes to whoever asks -- including a
		// disruptive isolated node, which is the exact failure mode
		// pre-vote exists to prevent.
		return PreVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: false}
	}

	if time.Since(r.lastHeartbeatSeen) < r.cfg.ElectionTimeoutBase {
		// We've heard from a leader recently enough that we wouldn't be
		// timing out ourselves -- no reason to encourage a disruptive election.
		return PreVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: false}
	}

	lastIndex, lastTerm := r.log.LastIndexAndTerm()
	logIsUpToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	return PreVoteResponse{Term: r.persisted.CurrentTerm, VoteGranted: logIsUpToDate}
}

// runPreVote asks every peer for a pre-vote and returns true only if a
// quorum (including this node's own implicit yes-vote for itself) says
// yes. With zero peers -- a single-node cluster -- quorum is 1 and this
// node's own vote already satisfies it, so a 1-node cluster proceeds to a
// real election without ever sending an RPC. That's intentional: there's
// no one to ask, and no one whose disruption pre-vote would need to guard
// against.
func (r *RaftNode) runPreVote() bool {
	r.mu.Lock()
	nextTerm := r.persisted.CurrentTerm + 1
	lastIndex, lastTerm := r.log.LastIndexAndTerm()
	candidateID := r.id
	peers := append([]string(nil), r.peers...)
	rpcTimeout := r.cfg.RPCTimeout
	r.mu.Unlock()

	quorumNeeded := len(peers)/2 + 1
	if len(peers) == 0 {
		return true // nothing to ask, and quorum of self is trivially met
	}

	type result struct{ granted bool }
	resultsC := make(chan result, len(peers))

	for _, peer := range peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()
			resp, err := r.transport.SendPreVote(ctx, peer, PreVoteRequest{
				NextTerm:     nextTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			resultsC <- result{granted: err == nil && resp.VoteGranted}
		}()
	}

	granted := 1 // self
	for i := 0; i < len(peers); i++ {
		res := <-resultsC
		if res.granted {
			granted++
		}
	}
	return granted >= quorumNeeded
}
