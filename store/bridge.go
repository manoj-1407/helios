package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manoj-1407/helios/raft"
)

type StatsSnapshot struct {
	WriteAmplification float64 `json:"write_amplification"`
	SstableCount       int     `json:"sstable_count"`
	CompactionEvents   int64   `json:"compaction_events"`
	Strategy           string  `json:"strategy"`
	StrategyChanged    bool    `json:"strategy_changed"`
}

type DataPoint struct {
	Timestamp          time.Time
	CompactionEventSaw bool
	ReplicationLagNs   int64
}

type BridgeMetrics struct {
	mu sync.Mutex

	ApplyCount       atomic.Int64
	ApplyErrCount    atomic.Int64
	CompactionEvents atomic.Int64

	// core thesis data: every apply records its latency and whether a
	// compaction strategy switch was observed on that stats poll.
	// replication_bench.go reads this to compute lag-vs-compaction correlation.
	Points []DataPoint
}

func (m *BridgeMetrics) record(lagNs int64, compactionSaw bool) {
	m.mu.Lock()
	m.Points = append(m.Points, DataPoint{
		Timestamp:          time.Now(),
		CompactionEventSaw: compactionSaw,
		ReplicationLagNs:   lagNs,
	})
	m.mu.Unlock()
}

func (m *BridgeMetrics) Snapshot() []DataPoint {
	m.mu.Lock()
	out := make([]DataPoint, len(m.Points))
	copy(out, m.Points)
	m.mu.Unlock()
	return out
}

// HeliosKVBridge implements raft.StateMachine by forwarding committed log
// entries to a helios-kv HTTP server (HeliosServer.java). One bridge per
// Raft node — leader and each follower each talk to their own local JVM.
// Keeping them separate is what makes the per-node compaction strategy
// comparison meaningful: each node can run ADAPTIVE, LEVELED, or TIERED
// independently, and we measure whether the follower's strategy affects
// how fast it can apply entries (and therefore how much it lags).
type HeliosKVBridge struct {
	baseURL string
	token   string // optional shared secret; empty means no auth
	client  *http.Client
	Metrics *BridgeMetrics

	lastCompactionEvents int64
}

func NewHeliosKVBridge(addr string) *HeliosKVBridge {
	return NewHeliosKVBridgeWithAuth(addr, "")
}

func NewHeliosKVBridgeWithAuth(addr, token string) *HeliosKVBridge {
	return &HeliosKVBridge{
		baseURL: "http://" + addr,
		token:   token,
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		Metrics: &BridgeMetrics{},
	}
}

// setAuthHeader attaches X-Helios-Token to the request when a token is configured.
func (b *HeliosKVBridge) setAuthHeader(req *http.Request) {
	if b.token != "" {
		req.Header.Set("X-Helios-Token", b.token)
	}
}

func (b *HeliosKVBridge) Apply(entry raft.LogEntry) error {
	start := time.Now()

	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		b.Metrics.ApplyErrCount.Add(1)
		return fmt.Errorf("bridge: decode command index=%d: %w", entry.Index, err)
	}

	switch cmd.Type {
	case CmdPut:
		err = b.post("/put", cmd.Key+"\n"+cmd.Value)
	case CmdDelete:
		err = b.post("/delete", cmd.Key)
	default:
		return fmt.Errorf("bridge: unknown command type %d", cmd.Type)
	}

	lagNs := time.Since(start).Nanoseconds()
	b.Metrics.ApplyCount.Add(1)

	compactionSaw := false
	if stats, serr := b.fetchStats(); serr == nil {
		if stats.CompactionEvents != b.lastCompactionEvents {
			b.Metrics.CompactionEvents.Add(stats.CompactionEvents - b.lastCompactionEvents)
			b.lastCompactionEvents = stats.CompactionEvents
			compactionSaw = true
		}
	}

	b.Metrics.record(lagNs, compactionSaw)

	if err != nil {
		b.Metrics.ApplyErrCount.Add(1)
		return fmt.Errorf("bridge: apply index=%d: %w", entry.Index, err)
	}
	return nil
}

func (b *HeliosKVBridge) Get(ctx context.Context, key string) (string, bool, error) {
	body, status, err := b.postRaw("/get", key)
	if err != nil {
		return "", false, err
	}
	if status == 404 {
		return "", false, nil
	}
	return string(body), true, nil
}

// Snapshot flushes the helios-kv MemTable and returns a tar.gz of the
// entire SSTable data directory. The Raft layer calls this on the leader
// to produce snapshot bytes that are shipped to lagging followers via
// InstallSnapshot. The /snapshot endpoint in HeliosServer.java handles
// the flush + archive; we just stream the response body here.
func (b *HeliosKVBridge) Snapshot() ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, b.baseURL+"/snapshot", nil)
	if err != nil {
		return nil, fmt.Errorf("bridge: snapshot: build request: %w", err)
	}
	b.setAuthHeader(req)
	// Snapshot can be slow (flushing MemTable + creating tar.gz of all SSTables).
	// Use a dedicated long-timeout client rather than the normal 2s one.
	snapClient := &http.Client{Timeout: 120 * time.Second}
	resp, err := snapClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bridge: snapshot: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bridge: snapshot: status %d: %s", resp.StatusCode, body)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("bridge: snapshot: read body: %w", err)
	}
	return data, nil
}

// Restore replaces this node's storage state with a snapshot received from
// the leader. It POSTs the tar.gz to /restore on the local JVM, which stops
// the DB, unpacks the archive over its data directory, and reopens the engine.
func (b *HeliosKVBridge) Restore(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, b.baseURL+"/restore", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("bridge: restore: build request: %w", err)
	}
	b.setAuthHeader(req)
	req.Header.Set("Content-Type", "application/octet-stream")
	// Restore also involves decompressing and writing potentially many SSTables.
	restoreClient := &http.Client{Timeout: 120 * time.Second}
	resp, err := restoreClient.Do(req)
	if err != nil {
		return fmt.Errorf("bridge: restore: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("bridge: restore: status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (b *HeliosKVBridge) Health(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, b.baseURL+"/health", nil)
	b.setAuthHeader(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("health check: status %d", resp.StatusCode)
	}
	return nil
}

func (b *HeliosKVBridge) fetchStats() (*StatsSnapshot, error) {
	req, _ := http.NewRequest(http.MethodGet, b.baseURL+"/stats", nil)
	b.setAuthHeader(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var s StatsSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (b *HeliosKVBridge) post(path, body string) error {
	_, _, err := b.postRaw(path, body)
	return err
}

func (b *HeliosKVBridge) postRaw(path, body string) ([]byte, int, error) {
	req, err := http.NewRequest(http.MethodPost, b.baseURL+path, bytes.NewBufferString(body))
	if err != nil {
		return nil, 0, err
	}
	b.setAuthHeader(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if resp.StatusCode >= 500 {
		return nil, resp.StatusCode, fmt.Errorf("helios-kv server error %d: %s", resp.StatusCode, out)
	}
	return out, resp.StatusCode, nil
}

// PushPressure sends the current replication pressure level (0.0-1.0) to
// the helios-kv JVM so it can scale its compaction aggressiveness accordingly.
// This is the feedback loop: Raft replication state → storage compaction behavior.
// Called by the CompactionPressureLoop whenever pressure changes by >0.1.
func (b *HeliosKVBridge) PushPressure(pressure float64) error {
	body := fmt.Sprintf("%.4f", pressure)
	_, _, err := b.postRaw("/pressure", body)
	return err
}

func (b *HeliosKVBridge) Token() string { return b.token }

func (b *HeliosKVBridge) FetchStats() (*StatsSnapshot, error) {
	return b.fetchStats()
}
