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
	ID             string  `json:"id"`
	State          string  `json:"state"`
	Term           uint64  `json:"term"`
	CommitIndex    uint64  `json:"commit_index"`
	Strategy       string  `json:"strategy"`
	WriteAmp       float64 `json:"write_amp"`
	SstableCount   int     `json:"sstable_count"`
	PressureLevel  float64 `json:"pressure_level"`
	FsyncLatencyNs int64   `json:"fsync_latency_ns"`
}

type ClusterSnapshot struct {
	Timestamp int64          `json:"ts"`
	Nodes     []NodeSnapshot `json:"nodes"`
	LeaderID  string         `json:"leader_id"`
	MaxLag    uint64         `json:"max_lag"`
}

func NewServer(nodes []*raft.RaftNode, bridges []*store.HeliosKVBridge, addr string) *Server {
	return &Server{nodes: nodes, bridges: bridges, addr: addr}
}

func (s *Server) Start() {
	go s.pollLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/events", s.handleSSE)
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
		if len(s.history) > 300 {
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
			ID:            node.ID(),
			State:         node.State().String(),
			Term:          node.Term(),
			CommitIndex:   node.CommitIndex(),
			PressureLevel: node.RepMetrics.PressureLevel(),
		}
		if node.State() == raft.Leader {
			leaderID = node.ID()
			lag := node.RepMetrics.MaxFollowerLag()
			if lag > maxLag {
				maxLag = lag
			}
		}
		if i < len(s.bridges) {
			if st, err := s.bridges[i].FetchStats(); err == nil && st != nil {
				ns.Strategy = st.Strategy
				ns.WriteAmp = st.WriteAmplification
				ns.SstableCount = st.SstableCount
			}
		}
		snap.Nodes = append(snap.Nodes, ns)
	}
	snap.LeaderID = leaderID
	snap.MaxLag = maxLag
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

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}

	// send history first so the charts populate immediately on page load
	s.mu.RLock()
	hist := make([]ClusterSnapshot, len(s.history))
	copy(hist, s.history)
	s.mu.RUnlock()
	for _, h := range hist {
		data, _ := json.Marshal(h)
		fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

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
<title>Helios</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap">
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root {
  --bg:       #080E1C;
  --surface:  #0D1528;
  --border:   #1A2540;
  --border2:  #243054;
  --text:     #C8D8F0;
  --muted:    #5A7090;
  --leader:   #00E0A0;
  --follower: #4D9FFF;
  --cand:     #FFB020;
  --danger:   #FF4747;
  --accent:   #4D9FFF;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body {
  font-family: 'Inter', system-ui, sans-serif;
  background: var(--bg);
  color: var(--text);
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  font-size: 13px;
}

/* ── header ── */
#hdr {
  display: flex;
  align-items: center;
  gap: 20px;
  padding: 0 24px;
  height: 48px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}
#hdr-logo {
  font-family: 'JetBrains Mono', monospace;
  font-size: 14px;
  font-weight: 500;
  color: #fff;
  letter-spacing: 0.04em;
}
.hdr-pill {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 3px 10px;
  border-radius: 3px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  font-weight: 500;
  border: 1px solid var(--border2);
  color: var(--muted);
}
.hdr-pill.live { border-color: var(--leader); color: var(--leader); }
.hdr-pill.live::before {
  content: '';
  width: 6px; height: 6px;
  border-radius: 50%;
  background: var(--leader);
  animation: pulse 2s infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}
.hdr-pill.warn { border-color: var(--cand); color: var(--cand); }
#hdr-time { margin-left: auto; font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--muted); }

/* ── main layout ── */
#main {
  display: flex;
  flex: 1;
  overflow: hidden;
}

/* ── node sidebar ── */
#sidebar {
  width: 256px;
  flex-shrink: 0;
  border-right: 1px solid var(--border);
  overflow-y: auto;
  padding: 16px 0;
}
.sidebar-label {
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  color: var(--muted);
  padding: 0 16px 8px;
  text-transform: uppercase;
}
.node-card {
  position: relative;
  padding: 12px 16px;
  border-left: 3px solid transparent;
  margin-bottom: 2px;
  transition: background 0.2s;
}
.node-card.leader   { border-left-color: var(--leader); background: rgba(0,224,160,0.04); }
.node-card.follower { border-left-color: var(--follower); }
.node-card.candidate { border-left-color: var(--cand); }
.node-card:hover { background: rgba(77,159,255,0.06); }

.node-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
.node-id { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 500; color: #fff; }
.state-badge {
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.06em;
  padding: 2px 6px;
  border-radius: 2px;
  text-transform: uppercase;
}
.state-badge.leader   { background: rgba(0,224,160,0.15); color: var(--leader); }
.state-badge.follower { background: rgba(77,159,255,0.12); color: var(--follower); }
.state-badge.candidate{ background: rgba(255,176,32,0.15); color: var(--cand); }

.node-stats { display: flex; flex-direction: column; gap: 3px; }
.node-stat { display: flex; justify-content: space-between; align-items: center; }
.ns-label { font-size: 11px; color: var(--muted); }
.ns-val { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text); }

.pressure-row { margin-top: 8px; }
.pressure-track { height: 3px; background: var(--border2); border-radius: 2px; margin-top: 4px; overflow: hidden; }
.pressure-fill { height: 100%; border-radius: 2px; transition: width 0.6s, background 0.6s; }

/* ── content area ── */
#content {
  flex: 1;
  overflow-y: auto;
  padding: 20px 24px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

/* ── metric grid ── */
.metric-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
}
.metric-tile {
  background: var(--surface);
  border: 1px solid var(--border);
  padding: 14px 16px;
}
.mt-label { font-size: 10px; color: var(--muted); letter-spacing: 0.04em; margin-bottom: 6px; text-transform: uppercase; }
.mt-value { font-family: 'JetBrains Mono', monospace; font-size: 22px; font-weight: 500; color: #fff; line-height: 1; }
.mt-sub { font-size: 10px; color: var(--muted); margin-top: 4px; }
.mt-value.ok   { color: var(--leader); }
.mt-value.warn { color: var(--cand); }
.mt-value.bad  { color: var(--danger); }

/* ── chart panels ── */
.chart-panel {
  background: var(--surface);
  border: 1px solid var(--border);
  padding: 16px 20px 12px;
}
.cp-header { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 14px; }
.cp-title { font-size: 11px; font-weight: 600; color: var(--text); letter-spacing: 0.04em; }
.cp-meta { font-size: 10px; color: var(--muted); font-family: 'JetBrains Mono', monospace; }
.chart-wrap { position: relative; height: 140px; }
.chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }

/* ── log table ── */
.log-panel {
  background: var(--surface);
  border: 1px solid var(--border);
  padding: 16px 20px;
}
.log-table { width: 100%; border-collapse: collapse; }
.log-table th {
  text-align: left;
  font-size: 10px;
  letter-spacing: 0.06em;
  color: var(--muted);
  padding: 0 0 8px;
  border-bottom: 1px solid var(--border);
  text-transform: uppercase;
}
.log-table td {
  padding: 7px 0;
  border-bottom: 1px solid var(--border);
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: var(--text);
  vertical-align: middle;
}
.log-table td:not(:first-child) { padding-left: 16px; }
.log-table th:not(:first-child) { padding-left: 16px; }
.log-table tr:last-child td { border-bottom: none; }
.strategy-tag {
  display: inline-block;
  padding: 1px 6px;
  border: 1px solid var(--border2);
  border-radius: 2px;
  font-size: 10px;
  color: var(--muted);
}
.wamp-num { color: #fff; }
.bar-cell { width: 120px; }
.mini-bar { height: 4px; background: var(--border2); border-radius: 2px; overflow: hidden; }
.mini-fill { height: 100%; border-radius: 2px; transition: width 0.4s, background 0.4s; }

/* ── status footer ── */
#footer {
  border-top: 1px solid var(--border);
  padding: 7px 24px;
  display: flex;
  gap: 20px;
  align-items: center;
  flex-shrink: 0;
}
.ft-item { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: var(--muted); }
.ft-item span { color: var(--text); }
</style>
</head>
<body>

<header id="hdr">
  <div id="hdr-logo">⬡ helios</div>
  <div class="hdr-pill live" id="conn-pill">connected</div>
  <div class="hdr-pill" id="leader-pill">leader —</div>
  <div class="hdr-pill" id="lag-pill">lag —</div>
  <div id="hdr-time">—</div>
</header>

<div id="main">
  <aside id="sidebar">
    <div class="sidebar-label">nodes</div>
    <div id="node-list"></div>
  </aside>

  <section id="content">
    <div class="metric-row" id="metric-row"></div>

    <div class="chart-row">
      <div class="chart-panel">
        <div class="cp-header">
          <span class="cp-title">write amplification</span>
          <span class="cp-meta" id="wamp-now">—</span>
        </div>
        <div class="chart-wrap"><canvas id="wamp-chart"></canvas></div>
      </div>
      <div class="chart-panel">
        <div class="cp-header">
          <span class="cp-title">replication pressure</span>
          <span class="cp-meta" id="pres-now">—</span>
        </div>
        <div class="chart-wrap"><canvas id="pres-chart"></canvas></div>
      </div>
    </div>

    <div class="log-panel">
      <div class="cp-header" style="margin-bottom:10px">
        <span class="cp-title">node details</span>
      </div>
      <table class="log-table">
        <thead>
          <tr>
            <th>node</th>
            <th>state</th>
            <th>term</th>
            <th>commit</th>
            <th>strategy</th>
            <th>write amp</th>
            <th>sstables</th>
            <th>pressure</th>
          </tr>
        </thead>
        <tbody id="detail-table"></tbody>
      </table>
    </div>
  </section>
</div>

<footer id="footer">
  <div class="ft-item">updated <span id="ft-ts">—</span></div>
  <div class="ft-item">nodes <span id="ft-nodes">—</span></div>
  <div class="ft-item">max lag <span id="ft-lag">—</span> entries</div>
  <div class="ft-item">uptime <span id="ft-uptime">—</span></div>
</footer>

<script>
const NODE_COLORS = ['#4D9FFF','#00E0A0','#FFB020','#FF7A7A','#B47FFF','#FF9F4D'];
const MAX_POINTS = 300;
const startTs = Date.now();

let wampChart = null, presChart = null;
let initialized = false;
let nodeIds = [];

function pcolor(p) {
  if (p < 0.3) return '#00E0A0';
  if (p < 0.6) return '#FFB020';
  if (p < 0.85) return '#FF7A7A';
  return '#FF4747';
}

function stateClass(s) {
  const l = (s||'').toLowerCase();
  if (l === 'leader') return 'leader';
  if (l === 'candidate') return 'candidate';
  return 'follower';
}

function makeChart(id, label) {
  const ctx = document.getElementById(id).getContext('2d');
  return new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          display: true,
          labels: {
            color: '#5A7090',
            font: { family: "'JetBrains Mono', monospace", size: 10 },
            boxWidth: 10, boxHeight: 2,
            padding: 12
          }
        },
        tooltip: {
          backgroundColor: '#0D1528',
          borderColor: '#1A2540',
          borderWidth: 1,
          titleColor: '#C8D8F0',
          bodyColor: '#5A7090',
          bodyFont: { family: "'JetBrains Mono', monospace", size: 10 }
        }
      },
      scales: {
        x: {
          ticks: {
            color: '#3A5070',
            font: { family: "'JetBrains Mono', monospace", size: 9 },
            maxTicksLimit: 6,
            maxRotation: 0
          },
          grid: { color: 'rgba(26,37,64,0.8)', drawBorder: false }
        },
        y: {
          min: 0,
          ticks: {
            color: '#3A5070',
            font: { family: "'JetBrains Mono', monospace", size: 9 }
          },
          grid: { color: 'rgba(26,37,64,0.8)', drawBorder: false }
        }
      }
    }
  });
}

function initCharts(nodes) {
  nodeIds = nodes.map(n => n.id);
  wampChart = makeChart('wamp-chart', 'write amp');
  presChart  = makeChart('pres-chart',  'pressure');
  nodes.forEach((n, i) => {
    const color = NODE_COLORS[i % NODE_COLORS.length];
    wampChart.data.datasets.push({
      label: n.id,
      data: [],
      borderColor: color,
      backgroundColor: 'transparent',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3
    });
    presChart.data.datasets.push({
      label: n.id,
      data: [],
      borderColor: color,
      backgroundColor: 'transparent',
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3
    });
  });
}

function pushChartPoint(snap) {
  const ts = new Date(snap.ts).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});
  [wampChart, presChart].forEach(c => {
    c.data.labels.push(ts);
    if (c.data.labels.length > MAX_POINTS) c.data.labels.shift();
  });
  snap.nodes.forEach((n, i) => {
    if (i < wampChart.data.datasets.length) {
      wampChart.data.datasets[i].data.push(n.write_amp);
      if (wampChart.data.datasets[i].data.length > MAX_POINTS) wampChart.data.datasets[i].data.shift();
    }
    if (i < presChart.data.datasets.length) {
      presChart.data.datasets[i].data.push(parseFloat((n.pressure_level*100).toFixed(1)));
      if (presChart.data.datasets[i].data.length > MAX_POINTS) presChart.data.datasets[i].data.shift();
    }
  });
  wampChart.update('none');
  presChart.update('none');
}

function renderSidebar(snap) {
  const list = document.getElementById('node-list');
  list.innerHTML = snap.nodes.map((n, i) => {
    const sc = stateClass(n.state);
    const pct = (n.pressure_level * 100).toFixed(0);
    const pc = pcolor(n.pressure_level);
    return ` + "`" + `
    <div class="node-card ${sc}">
      <div class="node-header">
        <span class="node-id">${n.id}</span>
        <span class="state-badge ${sc}">${n.state}</span>
      </div>
      <div class="node-stats">
        <div class="node-stat">
          <span class="ns-label">term</span>
          <span class="ns-val">${n.term}</span>
        </div>
        <div class="node-stat">
          <span class="ns-label">commit</span>
          <span class="ns-val">${n.commit_index}</span>
        </div>
        <div class="node-stat">
          <span class="ns-label">sstables</span>
          <span class="ns-val">${n.sstable_count}</span>
        </div>
        <div class="node-stat">
          <span class="ns-label">write amp</span>
          <span class="ns-val">${n.write_amp.toFixed(3)}x</span>
        </div>
      </div>
      <div class="pressure-row">
        <div class="node-stat">
          <span class="ns-label">pressure</span>
          <span class="ns-val" style="color:${pc}">${pct}%</span>
        </div>
        <div class="pressure-track">
          <div class="pressure-fill" style="width:${pct}%;background:${pc}"></div>
        </div>
      </div>
    </div>` + "`" + `;
  }).join('');
}

function renderMetricRow(snap) {
  const nodes = snap.nodes;
  const leader = nodes.find(n => n.id === snap.leader_id) || nodes[0];
  const avgWamp = nodes.reduce((s,n) => s+n.write_amp,0)/Math.max(nodes.length,1);
  const maxPres = Math.max(...nodes.map(n=>n.pressure_level));
  const totalSst = nodes.reduce((s,n) => s+n.sstable_count,0);

  let lagClass = 'ok', lagDisplay = snap.max_lag + '';
  if (snap.max_lag > 100) lagClass = 'warn';
  if (snap.max_lag > 1000) lagClass = 'bad';

  let presClass = 'ok';
  if (maxPres > 0.3) presClass = 'warn';
  if (maxPres > 0.75) presClass = 'bad';

  document.getElementById('metric-row').innerHTML = ` + "`" + `
    <div class="metric-tile">
      <div class="mt-label">leader</div>
      <div class="mt-value ok">${snap.leader_id || '—'}</div>
      <div class="mt-sub">term ${leader ? leader.term : '—'}</div>
    </div>
    <div class="metric-tile">
      <div class="mt-label">max lag</div>
      <div class="mt-value ${lagClass}">${lagDisplay}</div>
      <div class="mt-sub">entries behind leader</div>
    </div>
    <div class="metric-tile">
      <div class="mt-label">avg write amp</div>
      <div class="mt-value">${avgWamp.toFixed(3)}x</div>
      <div class="mt-sub">across ${nodes.length} nodes</div>
    </div>
    <div class="metric-tile">
      <div class="mt-label">max pressure</div>
      <div class="mt-value ${presClass}">${(maxPres*100).toFixed(0)}%</div>
      <div class="mt-sub">${totalSst} total sstables</div>
    </div>` + "`" + `;
}

function renderDetailTable(snap) {
  document.getElementById('detail-table').innerHTML = snap.nodes.map((n,i) => {
    const sc = stateClass(n.state);
    const pct = (n.pressure_level*100).toFixed(1);
    const pc = pcolor(n.pressure_level);
    const color = NODE_COLORS[i % NODE_COLORS.length];
    return ` + "`" + `<tr>
      <td style="color:${color};font-weight:500">${n.id}</td>
      <td><span class="state-badge ${sc}">${n.state}</span></td>
      <td>${n.term}</td>
      <td>${n.commit_index}</td>
      <td><span class="strategy-tag">${n.strategy||'—'}</span></td>
      <td class="wamp-num">${n.write_amp.toFixed(4)}x</td>
      <td>${n.sstable_count}</td>
      <td class="bar-cell">
        <div style="display:flex;align-items:center;gap:6px">
          <div class="mini-bar" style="flex:1">
            <div class="mini-fill" style="width:${pct}%;background:${pc}"></div>
          </div>
          <span style="font-size:10px;color:${pc};width:32px;text-align:right">${pct}%</span>
        </div>
      </td>
    </tr>` + "`" + `;
  }).join('');
}

function renderHeader(snap) {
  document.getElementById('leader-pill').textContent = 'leader ' + (snap.leader_id || '—');
  const lagPill = document.getElementById('lag-pill');
  lagPill.textContent = 'lag ' + snap.max_lag;
  lagPill.className = 'hdr-pill' + (snap.max_lag > 100 ? ' warn' : '');
  document.getElementById('hdr-time').textContent = new Date(snap.ts).toLocaleTimeString();
  document.getElementById('ft-ts').textContent = new Date(snap.ts).toLocaleTimeString();
  document.getElementById('ft-nodes').textContent = snap.nodes.length;
  document.getElementById('ft-lag').textContent = snap.max_lag;
  const upMs = Date.now() - startTs;
  const upMin = Math.floor(upMs/60000), upSec = Math.floor((upMs%60000)/1000);
  document.getElementById('ft-uptime').textContent = upMin + 'm ' + upSec + 's';

  const avgWamp = snap.nodes.reduce((s,n)=>s+n.write_amp,0)/Math.max(snap.nodes.length,1);
  const maxPres = Math.max(...snap.nodes.map(n=>n.pressure_level));
  document.getElementById('wamp-now').textContent = avgWamp.toFixed(3) + 'x avg';
  document.getElementById('pres-now').textContent = (maxPres*100).toFixed(0) + '% peak';
}

function render(snap) {
  if (!initialized && snap.nodes && snap.nodes.length > 0) {
    initCharts(snap.nodes);
    initialized = true;
  }
  renderHeader(snap);
  renderSidebar(snap);
  renderMetricRow(snap);
  renderDetailTable(snap);
  if (initialized) pushChartPoint(snap);
}

const es = new EventSource('/events');
es.onmessage = e => { try { render(JSON.parse(e.data)); } catch(_) {} };
es.onerror = () => {
  const p = document.getElementById('conn-pill');
  p.textContent = 'reconnecting';
  p.className = 'hdr-pill warn';
};
es.onopen = () => {
  const p = document.getElementById('conn-pill');
  p.textContent = 'connected';
  p.className = 'hdr-pill live';
};
</script>
</body>
</html>`
