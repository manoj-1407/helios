// Package client provides a Go SDK for helios.
// Usage:
//
//	c, err := client.New([]string{"localhost:7379", "localhost:7380", "localhost:7381"})
//	err = c.Put(ctx, "key", "value")
//	val, err := c.Get(ctx, "key")
//	err = c.Delete(ctx, "key")
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client connects to a helios cluster. It automatically routes writes to
// the current leader and retries against other nodes if the leader changes.
type Client struct {
	mu      sync.RWMutex
	addrs   []string
	leader  string
	http    *http.Client
	timeout time.Duration
}

// New creates a client connected to a helios cluster.
func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("helios client: at least one address required")
	}
	c := &Client{
		addrs:   addrs,
		http:    &http.Client{Timeout: 5 * time.Second},
		timeout: 5 * time.Second,
	}
	c.discoverLeader()
	return c, nil
}

func (c *Client) discoverLeader() {
	for _, addr := range c.addrs {
		resp, err := c.http.Get(fmt.Sprintf("http://%s/health", addr))
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			c.mu.Lock()
			c.leader = addr
			c.mu.Unlock()
			return
		}
	}
}

// Put writes a key-value pair to the cluster.
func (c *Client) Put(ctx context.Context, key, value string) error {
	body := key + "\n" + value
	return c.writeWithRetry(ctx, "/put", body)
}

// Get reads a value by key. Returns ("", false, nil) if the key doesn't exist.
func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	addr := c.currentAddr()
	url := fmt.Sprintf("http://%s/get", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(key))
	if err != nil {
		return "", false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", false, fmt.Errorf("helios get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return "", false, nil
	}
	if resp.StatusCode != 200 {
		return "", false, fmt.Errorf("helios get: status %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	return string(b), true, nil
}

// Delete removes a key from the cluster.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.writeWithRetry(ctx, "/delete", key)
}

// Scan returns all live key-value pairs whose key starts with prefix.
// Empty prefix returns every key in the store. Reads from the current
// node (not necessarily the leader — reads are consistent at a single
// node; for linearizable reads, use the leader).
func (c *Client) Scan(ctx context.Context, prefix string) (map[string]string, error) {
	addr := c.currentAddr()
	url := fmt.Sprintf("http://%s/scan", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(prefix))
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("helios scan: status %d: %s", resp.StatusCode, body)
	}
	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("helios scan: decode: %w", err)
	}
	return result, nil
}

// writeWithRetry sends a write to the known leader and retries on 503.
func (c *Client) writeWithRetry(ctx context.Context, path, body string) error {
	tried := map[string]bool{}
	addr := c.currentAddr()

	for attempts := 0; attempts < len(c.addrs); attempts++ {
		if tried[addr] {
			addr = c.nextUntriedAddr(tried)
			if addr == "" {
				break
			}
		}
		tried[addr] = true

		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			fmt.Sprintf("http://%s%s", addr, path),
			strings.NewReader(body))
		if err != nil {
			return err
		}

		resp, err := c.http.Do(req)
		if err != nil {
			addr = c.nextUntriedAddr(tried)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == 200 {
			c.mu.Lock()
			c.leader = addr
			c.mu.Unlock()
			return nil
		}
		if resp.StatusCode == 503 {
			addr = c.nextUntriedAddr(tried)
			continue
		}
		return fmt.Errorf("helios write: unexpected status %d", resp.StatusCode)
	}
	return fmt.Errorf("helios write: no available leader found after trying all nodes")
}

func (c *Client) currentAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.leader != "" {
		return c.leader
	}
	return c.addrs[0]
}

func (c *Client) nextUntriedAddr(tried map[string]bool) string {
	for _, addr := range c.addrs {
		if !tried[addr] {
			return addr
		}
	}
	return ""
}
