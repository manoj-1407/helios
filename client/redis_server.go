package client

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// RESPServer implements Redis Serialization Protocol (RESP) so any existing
// redis client library (redis-cli, ioredis, jedis, go-redis, redis-py, etc.)
// can talk to helios without modification.
//
// Supported commands: SET, GET, DEL, PING, COMMAND (for redis-cli handshake).
// This covers the core KV use case. Helios is not a drop-in Redis replacement
// (no TTL, no pub/sub, no sorted sets) but for GET/SET/DEL workloads any
// redis client works against it unchanged.
//
// Why RESP and not HTTP: redis clients are ubiquitous, battle-tested, and
// have connection pooling, pipelining, and retry logic built in. Supporting
// the redis wire protocol means helios gets all of that for free.
type RESPServer struct {
	helios *Client
	addr   string
}

func NewRESPServer(heliosAddrs []string, listenAddr string) (*RESPServer, error) {
	c, err := New(heliosAddrs)
	if err != nil {
		return nil, err
	}
	return &RESPServer{helios: c, addr: listenAddr}, nil
}

// Serve starts the RESP TCP server. Blocks until the listener errors.
func (s *RESPServer) Serve() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("helios-redis: listen on %s: %w", s.addr, err)
	}
	fmt.Printf("[helios-redis] listening on %s (redis-wire compatible)\n", s.addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *RESPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(60 * time.Second))
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		conn.SetDeadline(time.Now().Add(60 * time.Second))
		cmd, args, err := readCommand(r)
		if err != nil {
			return
		}
		s.dispatch(cmd, args, w)
		w.Flush()
	}
}

func (s *RESPServer) dispatch(cmd string, args []string, w *bufio.Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch strings.ToUpper(cmd) {
	case "PING":
		if len(args) == 0 {
			writeSimple(w, "PONG")
		} else {
			writeBulk(w, args[0])
		}

	case "SET":
		if len(args) < 2 {
			writeError(w, "ERR wrong number of arguments for SET")
			return
		}
		if err := s.helios.Put(ctx, args[0], args[1]); err != nil {
			writeError(w, err.Error())
			return
		}
		writeSimple(w, "OK")

	case "GET":
		if len(args) < 1 {
			writeError(w, "ERR wrong number of arguments for GET")
			return
		}
		val, found, err := s.helios.Get(ctx, args[0])
		if err != nil {
			writeError(w, err.Error())
			return
		}
		if !found {
			writeNull(w)
			return
		}
		writeBulk(w, val)

	case "DEL":
		if len(args) < 1 {
			writeError(w, "ERR wrong number of arguments for DEL")
			return
		}
		deleted := 0
		for _, key := range args {
			if err := s.helios.Delete(ctx, key); err == nil {
				deleted++
			}
		}
		writeInt(w, deleted)

	case "COMMAND":
		// redis-cli sends COMMAND DOCS or COMMAND COUNT on connect.
		// Return an empty array to satisfy the handshake.
		fmt.Fprintf(w, "*0\r\n")

	case "QUIT":
		writeSimple(w, "OK")

	default:
		writeError(w, fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

// RESP protocol readers/writers

func readCommand(r *bufio.Reader) (string, []string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", nil, err
	}
	if len(line) == 0 {
		return "", nil, fmt.Errorf("empty line")
	}

	if line[0] == '*' {
		// Array format: *N\r\n then N bulk strings
		count, err := strconv.Atoi(line[1:])
		if err != nil || count <= 0 {
			return "", nil, fmt.Errorf("invalid array length")
		}
		parts := make([]string, 0, count)
		for i := 0; i < count; i++ {
			s, err := readBulkString(r)
			if err != nil {
				return "", nil, err
			}
			parts = append(parts, s)
		}
		if len(parts) == 0 {
			return "", nil, fmt.Errorf("empty command")
		}
		return parts[0], parts[1:], nil
	}

	// Inline format: "GET key\r\n"
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", nil, fmt.Errorf("empty inline command")
	}
	return parts[0], parts[1:], nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("expected bulk string, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return "", fmt.Errorf("invalid bulk string length")
	}
	buf := make([]byte, n+2) // +2 for \r\n
	if _, err := r.Read(buf); err != nil {
		return "", err
	}
	return string(buf[:n]), nil
}

func writeSimple(w *bufio.Writer, s string) { fmt.Fprintf(w, "+%s\r\n", s) }
func writeError(w *bufio.Writer, s string)  { fmt.Fprintf(w, "-%s\r\n", s) }
func writeInt(w *bufio.Writer, n int)       { fmt.Fprintf(w, ":%d\r\n", n) }
func writeNull(w *bufio.Writer)             { fmt.Fprintf(w, "$-1\r\n") }
func writeBulk(w *bufio.Writer, s string) {
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
}
