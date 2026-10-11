package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/manoj-1407/helios/raft"
)

type KVServer struct {
	mu     sync.RWMutex
	node   *raft.RaftNode
	bridge *HeliosKVBridge
	srv    *http.Server
}

func NewKVServer(node *raft.RaftNode, bridge *HeliosKVBridge, port int) *KVServer {
	s := &KVServer{node: node, bridge: bridge}
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/put",    s.handlePut)
	mux.HandleFunc("/kv/get",    s.handleGet)
	mux.HandleFunc("/kv/delete", s.handleDelete)
	mux.HandleFunc("/kv/stats",  s.handleStats)
	s.srv = &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	return s
}

func (s *KVServer) Start() error              { return s.srv.ListenAndServe() }
func (s *KVServer) Stop(ctx context.Context) error { return s.srv.Shutdown(ctx) }

type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *KVServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed); return
	}
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest); return
	}
	cmd := EncodeCommand(Command{Type: CmdPut, Key: req.Key, Value: req.Value})
	if err := s.propose(cmd); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable); return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *KVServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed); return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest); return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	val, found, err := s.bridge.Get(ctx, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError); return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"key": key, "value": val, "found": found})
}

func (s *KVServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed); return
	}
	var req struct{ Key string `json:"key"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest); return
	}
	cmd := EncodeCommand(Command{Type: CmdDelete, Key: req.Key})
	if err := s.propose(cmd); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable); return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *KVServer) handleStats(w http.ResponseWriter, r *http.Request) {
	m := s.bridge.Metrics
	json.NewEncoder(w).Encode(map[string]any{
		"apply_count":       m.ApplyCount.Load(),
		"apply_err_count":   m.ApplyErrCount.Load(),
		"compaction_events": m.CompactionEvents.Load(),
		"data_points":       len(m.Snapshot()),
	})
}

// propose: RaftNode.Propose takes (command []byte), returns (index, term, ok).
// No context parameter. Returns error only when not leader.
func (s *KVServer) propose(cmd []byte) error {
	_, _, ok := s.node.Propose(cmd)
	if !ok {
		return fmt.Errorf("not leader — retry")
	}
	return nil
}
