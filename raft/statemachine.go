package raft

import "sync"

// StateMachine is what Raft applies committed log entries to. helios-kv
// implements this interface via store/bridge.go (Week 3 scope -- gRPC
// sidecar to the Java process, not built yet). Keeping Raft's core state
// machine logic in raft.go completely decoupled from what the committed
// commands actually *mean* is what makes it possible to test replication,
// elections, and partition recovery without needing a running JVM at all.
type StateMachine interface {
	Apply(entry LogEntry) error
	// Snapshot serializes current state for log compaction. For helios-kv,
	// this means flushing the active MemTable and returning the resulting
	// SSTable path(s).
	Snapshot() ([]byte, error)
	// Restore installs a snapshot received from the leader.
	Restore(data []byte) error
}

// MockStateMachine is a trivial in-memory key-value map that satisfies
// StateMachine, used by every test in this package so raft.go's
// correctness can be verified independent of helios-kv ever being wired
// up. It records every applied command in order, which is exactly what
// TestLogReplication and the linearizability checker need to verify
// against.
type MockStateMachine struct {
	mu      sync.Mutex
	data    map[string]string
	applied []LogEntry
}

func NewMockStateMachine() *MockStateMachine {
	return &MockStateMachine{data: make(map[string]string)}
}

// Command is the decoded form of LogEntry.Command -- a tiny, deliberately
// simple encoding (not protobuf, since that's store/command.go's job once
// the real bridge exists) good enough for tests to assert against.
type Command struct {
	Type  CommandType
	Key   string
	Value string
}

type CommandType int

const (
	CmdPut CommandType = iota
	CmdDelete
)

func (m *MockStateMachine) Apply(entry LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cmd := decodeMockCommand(entry.Command)
	switch cmd.Type {
	case CmdPut:
		m.data[cmd.Key] = cmd.Value
	case CmdDelete:
		delete(m.data, cmd.Key)
	}
	m.applied = append(m.applied, entry)
	return nil
}

// AppliedCount is the thread-safe way to ask "how many entries has this
// state machine applied so far" -- exported as a field directly, this
// was a real, go-test--race-detected bug: Apply() mutates it under m.mu,
// and a test polling the raw field was reading it with no synchronization
// at all. The fix isn't "the test should have locked something it has no
// access to" -- it's that a mutable field guarded by an unexported lock
// should never have been exported as a field in the first place.
func (m *MockStateMachine) AppliedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.applied)
}

// AppliedEntries returns a copy, not a reference into internal state, so
// a caller iterating it can't race with a concurrent Apply().
func (m *MockStateMachine) AppliedEntries() []LogEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]LogEntry, len(m.applied))
	copy(out, m.applied)
	return out
}

func (m *MockStateMachine) Get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok
}

func (m *MockStateMachine) Snapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Minimal encoding good enough for round-tripping in tests: not meant
	// to be the real snapshot format, which will live in snapshot.go once
	// that's actually implemented.
	out := make([]byte, 0, 64)
	for k, v := range m.data {
		out = append(out, []byte(k)...)
		out = append(out, 0)
		out = append(out, []byte(v)...)
		out = append(out, 0, 0)
	}
	return out, nil
}

func (m *MockStateMachine) Restore(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string)
	parts := splitMockSnapshot(data)
	for i := 0; i+1 < len(parts); i += 2 {
		m.data[parts[i]] = parts[i+1]
	}
	return nil
}

func splitMockSnapshot(data []byte) []string {
	var out []string
	var cur []byte
	for i := 0; i < len(data); i++ {
		if data[i] == 0 {
			if i+1 < len(data) && data[i+1] == 0 {
				out = append(out, string(cur))
				cur = nil
				i++
				continue
			}
			out = append(out, string(cur))
			cur = nil
			continue
		}
		cur = append(cur, data[i])
	}
	return out
}

// encodeMockCommand / decodeMockCommand: a deliberately simple wire format
// (type byte, then key, NUL, value) -- this is test scaffolding, not the
// real command.go encoding store/command.go will use with protobuf.
func EncodeMockCommand(cmd Command) []byte {
	out := []byte{byte(cmd.Type)}
	out = append(out, []byte(cmd.Key)...)
	out = append(out, 0)
	out = append(out, []byte(cmd.Value)...)
	return out
}

func decodeMockCommand(data []byte) Command {
	if len(data) == 0 {
		return Command{}
	}
	cmdType := CommandType(data[0])
	rest := data[1:]
	for i, b := range rest {
		if b == 0 {
			return Command{Type: cmdType, Key: string(rest[:i]), Value: string(rest[i+1:])}
		}
	}
	return Command{Type: cmdType, Key: string(rest)}
}
