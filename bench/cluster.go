package bench

import (
	"fmt"
	"os"
	"sync"

	"github.com/manoj-1407/helios/chaos"
	"github.com/manoj-1407/helios/raft"
	"github.com/manoj-1407/helios/store"
)

type BenchCluster struct {
	mu        sync.Mutex
	nodes     map[string]*raft.RaftNode
	bridges   map[string]*store.HeliosKVBridge
	transport *raft.ChannelTransport
	network   *chaos.SimulatedNetwork
}

func NewBenchCluster(bridges []*store.HeliosKVBridge) (*BenchCluster, error) {
	n := len(bridges)
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("node%d", i)
	}

	network := chaos.NewSimulatedNetwork()
	transport := raft.NewChannelTransport()
	transport.SetFaultInjector(network)

	bc := &BenchCluster{
		nodes:     make(map[string]*raft.RaftNode),
		bridges:   make(map[string]*store.HeliosKVBridge),
		transport: transport,
		network:   network,
	}

	for i, id := range ids {
		bc.bridges[id] = bridges[i]
		dataDir, err := os.MkdirTemp("", fmt.Sprintf("bench-raft-%s-*", id))
		if err != nil {
			return nil, err
		}
		// Bug fix: peers excludes self — passing self causes a node to
		// send heartbeats to itself and deadlock inside HandleAppendEntries
		// while holding r.mu.
		peers := make([]string, 0, n-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node, err := raft.NewRaftNode(id, peers, dataDir, transport, bridges[i], raft.DefaultConfig())
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", id, err)
		}
		bc.nodes[id] = node
		// Bug fix: Register takes only the node, not (id, node)
		transport.Register(node)
	}

	for _, node := range bc.nodes {
		node.Start()
	}
	return bc, nil
}

func (bc *BenchCluster) Leader() *raft.RaftNode {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	for _, node := range bc.nodes {
		if node.State() == raft.Leader {
			return node
		}
	}
	return nil
}

func (bc *BenchCluster) AllBridges() []*store.HeliosKVBridge {
	out := make([]*store.HeliosKVBridge, 0, len(bc.bridges))
	for _, b := range bc.bridges {
		out = append(out, b)
	}
	return out
}

func (bc *BenchCluster) Shutdown() {
	for _, node := range bc.nodes {
		node.Stop()
	}
}
