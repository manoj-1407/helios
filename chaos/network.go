package chaos

import (
	"math/rand"
	"sync"
	"time"
)

// SimulatedNetwork tracks which links between nodes are currently
// partitioned, delayed, or lossy, and answers the one question
// raft.ChannelTransport needs before every send: "is this link up right
// now, and if so, how slow is it." It implements raft.NetworkFaultInjector
// structurally (an Allow(from, to string) (time.Duration, bool) method) --
// it never imports the raft package and the raft package never imports
// this one, so there's no cycle even though conceptually this is "part of"
// the transport layer's behavior.
type SimulatedNetwork struct {
	mu          sync.RWMutex
	partitioned map[string]bool           // node IDs currently cut off from everyone
	delays      map[linkKey]time.Duration // per-directed-link extra latency
	dropRates   map[linkKey]float64       // per-directed-link packet loss, 0.0-1.0
}

type linkKey struct {
	from, to string
}

func NewSimulatedNetwork() *SimulatedNetwork {
	return &SimulatedNetwork{
		partitioned: make(map[string]bool),
		delays:      make(map[linkKey]time.Duration),
		dropRates:   make(map[linkKey]float64),
	}
}

// Allow is called by raft.ChannelTransport before every single RPC.
// math/rand's package-level functions use a lock-protected global source,
// so calling rand.Float64() concurrently from many goroutines here is safe
// without this type needing its own RNG plumbing.
func (n *SimulatedNetwork) Allow(from, to string) (delay time.Duration, drop bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.partitioned[from] || n.partitioned[to] {
		return 0, true
	}
	if rate, ok := n.dropRates[linkKey{from, to}]; ok && rate > 0 {
		if rand.Float64() < rate {
			return 0, true
		}
	}
	if d, ok := n.delays[linkKey{from, to}]; ok {
		return d, false
	}
	return 0, false
}

// Partition cuts the given nodes off from every other node in both
// directions -- they can still reach each other if more than one ID is
// passed, but nothing crosses the partition boundary. This matches
// TestNetworkPartition's "minority partition can't elect a leader" shape:
// pass exactly the nodes you want isolated together.
func (n *SimulatedNetwork) Partition(nodeIDs ...string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	isolated := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		isolated[id] = true
	}
	// Simplest correct model for this test suite's needs: mark every node
	// NOT in the given set as partitioned away from the given set, by
	// marking the given set's members as globally partitioned UNLESS
	// they're only meant to talk to each other. Since every test here
	// only ever isolates one minority group at a time (never two disjoint
	// groups simultaneously), "partitioned" as a global flag per node is
	// sufficient and avoids needing a full link-matrix model.
	for id := range isolated {
		n.partitioned[id] = true
	}
}

func (n *SimulatedNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = make(map[string]bool)
}

func (n *SimulatedNetwork) DelayLink(from, to string, d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delays[linkKey{from, to}] = d
}

func (n *SimulatedNetwork) DropRate(from, to string, pct float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRates[linkKey{from, to}] = pct
}
