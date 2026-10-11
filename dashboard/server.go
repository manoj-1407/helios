package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/manoj-1407/helios/raft"
	"github.com/manoj-1407/helios/store"
)

// Server serves the live web dashboard at http://localhost:8080.
// It polls all cluster nodes every second and streams JSON to the browser
// via Server-Sent Events (SSE) -- no websocket library needed, SSE works
// in every browser natively and is a single HTTP connection.
type Server struct {
	nodes   []*raft.RaftNode
	bridges []*store.HeliosKVBridge
	addr    string
	history []ClusterSnapshot
	mu      sync.RWMutex
}

type NodeSnapshot struct {
	ID              string  `json:"id"`
	State           string  `json:"state"`
	Term            uint64  `json:"term"`
	CommitIndex     uint64  `json:"commit_index"`
	Strategy        string  `json:"strategy"`
	WriteAmp        float64 `json:"write_amp"`
	SstableCount    int     `json:"sstable_count"`
	PressureLevel   float64 `json:"pressure_level"`
	FsyncLatencyNs  int64   `json:"fsync_latency_ns"`
}

type ClusterSnapshot struct {
	Timestamp   int64          `json:"ts"`
	Nodes       []NodeSnapshot `json:"nodes"`
	LeaderID    string         `json:"leader_id"`
	MaxLag      uint64         `json:"max_lag"`
}

func NewServer(nodes []*raft.RaftNode, bridges []*store.HeliosKVBridge, addr string) *Server {
	return &Server{nodes: nodes, bridges: bridges, addr: addr}
}

func (s *Server) Start() {
	go s.pollLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/",        s.handleIndex)
	mux.HandleFunc("/events",  s.handleSSE)
	mux.HandleFunc("/snapshot", s.handleSnapshot)
	fmt.Printf("[helios-dashboard] live at http://%s\n", s.addr)
	http.ListenAndServe(s.addr, mux)
}

func (s *Server) pollLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		snap := s.collect()
		s.mu.Lock()
		s.history = append(s.history, snap)
		if len(s.history) > 300 { // keep 5 minutes
			s.history = s.history[len(s.history)-300:]
		}
		s.mu.Unlock()
	}
}

func (s *Server) collect() ClusterSnapshot {
	snap := ClusterSnapshot{Timestamp: time.Now().UnixMilli()}
	var leaderID string
	var maxLag uint64

	for i, node := range s.nodes {
		ns := NodeSnapshot{
			ID:    node.ID(),
			State: node.State().String(),
			Term:  node.Term(),
			CommitIndex: node.CommitIndex(),
			PressureLevel: node.RepMetrics.PressureLevel(),
		}
		if node.State() == raft.Leader {
			leaderID = node.ID()
			lag := node.RepMetrics.MaxFollowerLag()
			if lag > maxLag { maxLag = lag }
		}
		if i < len(s.bridges) {
			if st, err := s.bridges[i].FetchStats(); err == nil && st != nil {
				ns.Strategy     = st.Strategy
				ns.WriteAmp     = st.WriteAmplification
				ns.SstableCount = st.SstableCount
			}
		}
		snap.Nodes = append(snap.Nodes, ns)
	}
	snap.LeaderID = leaderID
	snap.MaxLag   = maxLag
	return snap
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	if len(s.history) == 0 {
		w.Write([]byte("[]"))
		return
	}
	json.NewEncoder(w).Encode(s.history)
}

// handleSSE streams cluster snapshots as Server-Sent Events.
// The browser JS listens on /events and updates the UI in real time.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type",  "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection",    "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			snap := s.collect()
			data, _ := json.Marshal(snap)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Helios Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'SF Mono', 'Fira Code', monospace; background: #0d1117; color: #c9d1d9; min-height: 100vh; }
  header { padding: 20px 32px; border-bottom: 1px solid #21262d; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 18px; font-weight: 600; color: #f0f6fc; }
  .badge { font-size: 11px; padding: 2px 8px; border-radius: 12px; background: #21262d; color: #8b949e; }
  .badge.live { background: #1f3d2b; color: #3fb950; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; padding: 24px 32px; }
  .card { background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 20px; }
  .card h2 { font-size: 13px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px; }
  .node { display: flex; align-items: center; gap: 10px; padding: 10px 0; border-bottom: 1px solid #21262d; }
  .node:last-child { border-bottom: none; }
  .dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .dot.leader   { background: #3fb950; box-shadow: 0 0 6px #3fb950; }
  .dot.follower { background: #58a6ff; }
  .dot.candidate{ background: #f0883e; }
  .node-info { flex: 1; }
  .node-id { font-size: 13px; color: #f0f6fc; font-weight: 500; }
  .node-meta { font-size: 11px; color: #8b949e; margin-top: 2px; }
  .metric { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #21262d; }
  .metric:last-child { border-bottom: none; }
  .metric-label { font-size: 12px; color: #8b949e; }
  .metric-value { font-size: 13px; color: #f0f6fc; font-weight: 500; }
  .metric-value.warn { color: #f0883e; }
  .metric-value.danger { color: #f85149; }
  .bar-wrap { background: #21262d; border-radius: 4px; height: 6px; width: 120px; }
  .bar { height: 6px; border-radius: 4px; background: #3fb950; transition: width 0.5s, background 0.5s; }
  .chart-area { height: 120px; position: relative; margin-top: 8px; }
  canvas { width: 100%; height: 100%; }
  .status-bar { padding: 8px 32px; font-size: 11px; color: #8b949e; border-top: 1px solid #21262d; }
  .pressure-indicator { display: flex; align-items: center; gap: 8px; }
</style>
</head>
<body>

<header>
  <h1>⬡ Helios</h1>
  <span class="badge live" id="live-badge">● LIVE</span>
  <span class="badge" id="leader-badge">leader: —</span>
  <span class="badge" id="lag-badge">max lag: —</span>
</header>

<div class="grid">
  <div class="card">
    <h2>Cluster Nodes</h2>
    <div id="nodes-list"></div>
  </div>
  <div class="card">
    <h2>Compaction Strategy</h2>
    <div id="strategy-list"></div>
  </div>
  <div class="card">
    <h2>Replication Pressure</h2>
    <div id="pressure-list"></div>
  </div>
  <div class="card" style="grid-column: 1 / -1;">
    <h2>Write Amplification — 5 min</h2>
    <div class="chart-area"><canvas id="wamp-chart"></canvas></div>
  </div>
</div>

<div class="status-bar" id="status-bar">connecting...</div>

<script>
const history = { ts: [], wamp: [] };

function stateColor(s) {
  if (s === 'Leader') return 'leader';
  if (s === 'Candidate') return 'candidate';
  return 'follower';
}

function pressureColor(p) {
  if (p < 0.3) return '#3fb950';
  if (p < 0.6) return '#d29922';
  if (p < 0.85) return '#f0883e';
  return '#f85149';
}

function valueClass(p) {
  if (p < 0.3) return '';
  if (p < 0.6) return 'warn';
  return 'danger';
}

function render(snap) {
  // Leader badge
  document.getElementById('leader-badge').textContent = 'leader: ' + (snap.leader_id || '—');
  document.getElementById('lag-badge').textContent = 'max lag: ' + snap.max_lag + ' entries';

  // Nodes
  const nl = document.getElementById('nodes-list');
  nl.innerHTML = snap.nodes.map(n => ` + "`" + `
    <div class="node">
      <div class="dot ${stateColor(n.state)}"></div>
      <div class="node-info">
        <div class="node-id">${n.id} <small style="color:#8b949e">${n.state}</small></div>
        <div class="node-meta">term ${n.term} · commit ${n.commit_index} · ${n.sstable_count} sstables</div>
      </div>
    </div>` + "`" + `).join('');

  // Strategy
  const sl = document.getElementById('strategy-list');
  sl.innerHTML = snap.nodes.map(n => ` + "`" + `
    <div class="metric">
      <span class="metric-label">${n.id}</span>
      <span class="metric-value">${n.strategy || '—'}</span>
    </div>
    <div class="metric">
      <span class="metric-label">write amp</span>
      <span class="metric-value">${n.write_amp.toFixed(4)}x</span>
    </div>` + "`" + `).join('');

  // Pressure
  const pl = document.getElementById('pressure-list');
  pl.innerHTML = snap.nodes.map(n => {
    const pct = (n.pressure_level * 100).toFixed(1);
    const color = pressureColor(n.pressure_level);
    return ` + "`" + `
    <div class="metric">
      <span class="metric-label">${n.id}</span>
      <div class="pressure-indicator">
        <div class="bar-wrap"><div class="bar" style="width:${pct}%;background:${color}"></div></div>
        <span class="metric-value ${valueClass(n.pressure_level)}">${pct}%</span>
      </div>
    </div>` + "`" + `;
  }).join('');

  // Chart data
  history.ts.push(new Date(snap.ts).toLocaleTimeString());
  const avgWamp = snap.nodes.reduce((s, n) => s + n.write_amp, 0) / snap.nodes.length;
  history.wamp.push(avgWamp);
  if (history.ts.length > 300) { history.ts.shift(); history.wamp.shift(); }
  drawChart();

  document.getElementById('status-bar').textContent =
    'last update: ' + new Date(snap.ts).toLocaleTimeString() +
    ' · ' + snap.nodes.length + ' nodes · max lag ' + snap.max_lag;
}

function drawChart() {
  const canvas = document.getElementById('wamp-chart');
  const ctx = canvas.getContext('2d');
  canvas.width = canvas.offsetWidth;
  canvas.height = canvas.offsetHeight;
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (history.wamp.length < 2) return;
  const max = Math.max(...history.wamp, 1);
  ctx.strokeStyle = '#3fb950';
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  history.wamp.forEach((v, i) => {
    const x = (i / (history.wamp.length - 1)) * w;
    const y = h - (v / max) * h * 0.9;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();

  // Baseline label
  ctx.fillStyle = '#8b949e';
  ctx.font = '10px monospace';
  ctx.fillText('1.0x', 4, h - 4);
  ctx.fillText(max.toFixed(2) + 'x', 4, 12);
}

const es = new EventSource('/events');
es.onmessage = e => render(JSON.parse(e.data));
es.onerror = () => {
  document.getElementById('live-badge').textContent = '● RECONNECTING';
  document.getElementById('live-badge').style.background = '#3d2b1f';
  document.getElementById('live-badge').style.color = '#f0883e';
};
es.onopen = () => {
  document.getElementById('live-badge').textContent = '● LIVE';
  document.getElementById('live-badge').style.background = '#1f3d2b';
  document.getElementById('live-badge').style.color = '#3fb950';
};
</script>
</body>
</html>`
