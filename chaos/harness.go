package chaos

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/manoj-1407/helios/raft"
)

// Cluster spins up N RaftNodes in-process with a simulated network. No
// Docker, no real sockets -- transport is a Go-channel mesh through
// raft.ChannelTransport. This is what lets the whole test suite run in
// milliseconds-to-seconds instead of needing real machines, and what lets
// a chaos run partition/heal/kill/restart deterministically instead of
// fighting real OS scheduling jitter.
type Cluster struct {
	mu        sync.Mutex
	nodes     map[string]*raft.RaftNode
	sms       map[string]*raft.MockStateMachine
	dataDirs  map[string]string
	transport *raft.ChannelTransport
	network   *SimulatedNetwork
	cfg       raft.Config
	baseDir   string
}

// NewCluster creates n nodes (ids "node0".."node{n-1}"), each with its own
// on-disk data directory under a temp dir, all registered on a shared
// ChannelTransport with a fresh SimulatedNetwork (perfect network by
// default -- nothing is partitioned, delayed, or dropped until a test
// calls Partition/DelayLink/DropRate). All nodes are started before
// NewCluster returns.
func NewCluster(n int) (*Cluster, error) {
	return NewClusterWithConfig(n, fastTestConfig())
}

func NewClusterWithConfig(n int, cfg raft.Config) (*Cluster, error) {
	baseDir, err := os.MkdirTemp("", "helios-raft-cluster-*")
	if err != nil {
		return nil, fmt.Errorf("creating cluster temp dir: %w", err)
	}

	network := NewSimulatedNetwork()
	transport := raft.NewChannelTransport()
	transport.SetFaultInjector(network)

	c := &Cluster{
		nodes:     make(map[string]*raft.RaftNode),
		sms:       make(map[string]*raft.MockStateMachine),
		dataDirs:  make(map[string]string),
		transport: transport,
		network:   network,
		cfg:       cfg,
		baseDir:   baseDir,
	}

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node%d", i)
	}

	for i, id := range ids {
		peers := make([]string, 0, n-1)
		for j, other := range ids {
			if j != i {
				peers = append(peers, other)
			}
		}
		if err := c.addNodeLocked(id, peers); err != nil {
			return nil, err
		}
	}

	for _, node := range c.nodes {
		node.Start()
	}
	return c, nil
}

func (c *Cluster) addNodeLocked(id string, peers []string) error {
	dataDir := filepath.Join(c.baseDir, id)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("creating data dir for %s: %w", id, err)
	}
	sm := raft.NewMockStateMachine()
	node, err := raft.NewRaftNode(id, peers, dataDir, c.transport, sm, c.cfg)
	if err != nil {
		return fmt.Errorf("creating node %s: %w", id, err)
	}
	c.nodes[id] = node
	c.sms[id] = sm
	c.dataDirs[id] = dataDir
	c.transport.Register(node)
	return nil
}

// fastTestConfig keeps the whole test suite fast without changing any of
// the actual algorithm -- only the units of time involved.
func fastTestConfig() raft.Config {
	return raft.Config{
		ElectionTimeoutBase:   150 * time.Millisecond,
		ElectionTimeoutSpread: 100 * time.Millisecond,
		HeartbeatInterval:     20 * time.Millisecond,
		RPCTimeout:            40 * time.Millisecond,
	}
}

func (c *Cluster) Nodes() map[string]*raft.RaftNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]*raft.RaftNode, len(c.nodes))
	for k, v := range c.nodes {
		out[k] = v
	}
	return out
}

func (c *Cluster) Node(id string) *raft.RaftNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *Cluster) StateMachine(id string) *raft.MockStateMachine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sms[id]
}

// Leader returns the first node currently believing itself to be leader,
// or nil if none does (a freshly-started cluster mid-election, or a
// cluster with no quorum available, can legitimately have no leader for
// a while -- callers in tests should poll with a timeout, not call this
// once and assume).
func (c *Cluster) Leader() *raft.RaftNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if n.State() == raft.Leader {
			return n
		}
	}
	return nil
}

// WaitForLeader polls until exactly one leader is visible or the timeout
// elapses. Returns nil on timeout.
func (c *Cluster) WaitForLeader(timeout time.Duration) *raft.RaftNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != nil {
			return l
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil
}

// Partition cuts the given node IDs off from the rest of the cluster.
func (c *Cluster) Partition(nodeIDs ...string) {
	c.network.Partition(nodeIDs...)
}

func (c *Cluster) Heal() {
	c.network.Heal()
}

func (c *Cluster) DelayLink(from, to string, d time.Duration) {
	c.network.DelayLink(from, to, d)
}

func (c *Cluster) DropRate(from, to string, pct float64) {
	c.network.DropRate(from, to, pct)
}

// KillNode stops a node's timers/goroutines and removes it from the
// transport registry, simulating a process that's no longer running --
// distinct from Partition, which keeps the process "alive" but
// unreachable. On-disk state is left untouched, matching what a real
// process kill would leave behind.
func (c *Cluster) KillNode(id string) {
	c.mu.Lock()
	node := c.nodes[id]
	c.mu.Unlock()
	if node == nil {
		return
	}
	node.Stop()
	c.transport.Unregister(id)
}

// RestartNode constructs a brand new RaftNode against the same data
// directory (so it recovers persisted state and log exactly the way a
// real restarted process would) and re-registers it on the transport.
func (c *Cluster) RestartNode(id string) error {
	c.mu.Lock()
	dataDir, ok := c.dataDirs[id]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("unknown node %s", id)
	}
	var peers []string
	for otherID := range c.nodes {
		if otherID != id {
			peers = append(peers, otherID)
		}
	}
	sm := raft.NewMockStateMachine() // a real restart would also replay any local snapshot into the state machine -- not implemented until snapshot.go is (Week 3)
	c.mu.Unlock()

	node, err := raft.NewRaftNode(id, peers, dataDir, c.transport, sm, c.cfg)
	if err != nil {
		return fmt.Errorf("restarting node %s: %w", id, err)
	}

	c.mu.Lock()
	c.nodes[id] = node
	c.sms[id] = sm
	c.mu.Unlock()

	c.transport.Register(node)
	node.Start()
	return nil
}

// Close stops every node and removes the cluster's temp data directory.
func (c *Cluster) Close() {
	c.mu.Lock()
	nodes := make([]*raft.RaftNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	baseDir := c.baseDir
	c.mu.Unlock()

	for _, n := range nodes {
		n.Stop()
	}
	os.RemoveAll(baseDir)
}
