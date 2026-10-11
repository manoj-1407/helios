package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Transport is everything a RaftNode needs to talk to its peers. RaftNode
// never knows or cares whether a concrete implementation is sending real
// bytes over a real socket or just calling a function pointer in the same
// process -- that's the whole point of the interface, and it's what lets
// the entire test suite run against ChannelTransport in milliseconds
// instead of needing real network sockets and real timeouts.
type Transport interface {
	SendRequestVote(ctx context.Context, peer string, req RequestVoteRequest) (RequestVoteResponse, error)
	SendPreVote(ctx context.Context, peer string, req PreVoteRequest) (PreVoteResponse, error)
	SendAppendEntries(ctx context.Context, peer string, req AppendEntriesRequest) (AppendEntriesResponse, error)
	SendInstallSnapshot(ctx context.Context, peer string, req InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// NetworkFaultInjector is the seam chaos.SimulatedNetwork hooks into
// without raft importing chaos (which would be a cycle, since chaos
// imports raft to build clusters of RaftNodes). ChannelTransport checks
// this before every send; a nil injector means a perfect network -- no
// delay, no drops. Go's implicit interface satisfaction means
// chaos.SimulatedNetwork never needs to know this type exists by name,
// it just needs an Allow(from, to string) (time.Duration, bool) method.
type NetworkFaultInjector interface {
	Allow(from, to string) (delay time.Duration, drop bool)
}

// ChannelTransport dispatches RPCs by direct in-process method calls
// against a registry of known nodes, instead of marshaling bytes onto a
// socket. Each call still runs as if it were a real RPC -- through a
// context with a deadline -- so the calling code in raft.go doesn't need
// a special "this is a test" branch anywhere. The fault injector, when
// set, can add artificial latency or drop the call entirely, which is
// what lets the chaos package simulate partitions and slow links without
// touching raft.go at all.
type ChannelTransport struct {
	mu     sync.RWMutex
	nodes  map[string]*RaftNode
	faults NetworkFaultInjector
}

func NewChannelTransport() *ChannelTransport {
	return &ChannelTransport{nodes: make(map[string]*RaftNode)}
}

func (t *ChannelTransport) Register(node *RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node.id] = node
}

func (t *ChannelTransport) Unregister(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, id)
}

// SetFaultInjector is called by chaos.NewCluster to wire a SimulatedNetwork
// in. Safe to call after nodes are already registered -- it only affects
// future sends.
func (t *ChannelTransport) SetFaultInjector(f NetworkFaultInjector) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.faults = f
}

func (t *ChannelTransport) lookup(id string) (*RaftNode, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n, ok := t.nodes[id]
	return n, ok
}

// gate applies the fault injector (if any) before a send is allowed to
// proceed. Returns an error if the call should be dropped, otherwise
// blocks for the injected delay (respecting ctx cancellation) and
// returns nil.
func (t *ChannelTransport) gate(ctx context.Context, from, to string) error {
	t.mu.RLock()
	faults := t.faults
	t.mu.RUnlock()
	if faults == nil {
		return nil
	}
	delay, drop := faults.Allow(from, to)
	if drop {
		return fmt.Errorf("transport: link %s -> %s is dropping packets (simulated partition)", from, to)
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (t *ChannelTransport) SendRequestVote(ctx context.Context, peer string, req RequestVoteRequest) (RequestVoteResponse, error) {
	if err := t.gate(ctx, req.CandidateID, peer); err != nil {
		return RequestVoteResponse{}, err
	}
	node, ok := t.lookup(peer)
	if !ok {
		return RequestVoteResponse{}, fmt.Errorf("transport: unknown peer %s", peer)
	}
	return node.HandleRequestVote(req), nil
}

func (t *ChannelTransport) SendPreVote(ctx context.Context, peer string, req PreVoteRequest) (PreVoteResponse, error) {
	if err := t.gate(ctx, req.CandidateID, peer); err != nil {
		return PreVoteResponse{}, err
	}
	node, ok := t.lookup(peer)
	if !ok {
		return PreVoteResponse{}, fmt.Errorf("transport: unknown peer %s", peer)
	}
	return node.HandlePreVote(req), nil
}

func (t *ChannelTransport) SendAppendEntries(ctx context.Context, peer string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if err := t.gate(ctx, req.LeaderID, peer); err != nil {
		return AppendEntriesResponse{}, err
	}
	node, ok := t.lookup(peer)
	if !ok {
		return AppendEntriesResponse{}, fmt.Errorf("transport: unknown peer %s", peer)
	}
	return node.HandleAppendEntries(req), nil
}

func (t *ChannelTransport) SendInstallSnapshot(ctx context.Context, peer string, req InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	if err := t.gate(ctx, req.LeaderID, peer); err != nil {
		return InstallSnapshotResponse{}, err
	}
	node, ok := t.lookup(peer)
	if !ok {
		return InstallSnapshotResponse{}, fmt.Errorf("transport: unknown peer %s", peer)
	}
	return node.HandleInstallSnapshot(req), nil
}

// jitterDuration is used by raft.go for election timeout randomization --
// pulled out here since transport.go already imports math/rand and this
// avoids every caller re-deriving its own randomness source.
func jitterDuration(base, spread time.Duration) time.Duration {
	if spread <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(int64(spread)))
}
