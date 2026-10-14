# client — Go SDK and Redis-wire Server

The `client` package has two responsibilities:

1. **Go SDK** (`client.go`) — a typed Go client for Helios that application code can import
2. **Redis-wire server** (`redis_server.go`) — a TCP server that speaks RESP (Redis Serialization Protocol), making any Redis client work against Helios without modification

---

## Go SDK

### Import

```go
import "github.com/manoj-1407/helios/client"
```

### Creating a client

```go
c, err := client.New([]string{
    "127.0.0.1:7001",
    "127.0.0.1:7002",
    "127.0.0.1:7003",
})
if err != nil {
    log.Fatal(err)
}
```

Pass all node addresses. The client probes them on startup to find the current leader and caches it for subsequent calls.

### Operations

```go
ctx := context.Background()

// write
err = c.Put(ctx, "user:1001", `{"name":"Manoj","city":"Hyderabad"}`)

// read
value, found, err := c.Get(ctx, "user:1001")
if found {
    fmt.Println(value)  // {"name":"Manoj","city":"Hyderabad"}
}

// delete
err = c.Delete(ctx, "user:1001")
```

### Leader routing and retry

All writes must go to the Raft leader. The client tracks the last known leader and sends writes there first.

If the leader has changed (node crash, re-election), the JVM returns `503 Service Unavailable`. The client automatically retries against the next node in its address list, and so on until it finds the new leader or exhausts all nodes.

```
PUT request → node1 (known leader)
    503 ← not leader anymore
PUT request → node2
    200 ← success (node2 is new leader)

client caches node2 as leader for future requests
```

Reads are sent to the current known node (may be a follower). In a single-node deployment this is always the leader. In a multi-node deployment, reads from followers may be slightly stale — this is **eventual consistency for reads, linearizable consistency for writes**. Read-your-writes is guaranteed if you read from the same node you wrote to.

### Timeout

Default operation timeout is 5 seconds. This covers the HTTP round trip to helios-kv including potential leader retry.

---

## RESP server (Redis-wire)

`redis_server.go`

### What RESP is

RESP (Redis Serialization Protocol) is the protocol Redis uses over TCP. It is line-based, human-readable in its inline form, and trivially parseable.

A SET command in RESP array format looks like:

```
*3\r\n      ← 3 elements follow
$3\r\n      ← bulk string, 3 bytes
SET\r\n
$3\r\n      ← bulk string, 3 bytes
foo\r\n
$3\r\n      ← bulk string, 3 bytes
bar\r\n
```

A response:
```
+OK\r\n     ← simple string response
```

### Why implement RESP?

Every language ecosystem has a Redis client library — Python's `redis`, Node.js's `ioredis`, Java's Jedis, Go's `go-redis`, Ruby's `redis-rb`, PHP's Predis. By speaking RESP, Helios is immediately accessible from all of them without any custom client code.

A developer using Python can do:
```python
import redis
r = redis.Redis(host='localhost', port=6380)
r.set('key', 'value')
print(r.get('key'))
```

This connects to Helios, not Redis. Zero code changes.

### Starting the server

```go
srv, err := client.NewRESPServer(
    []string{"127.0.0.1:7001"},  // helios node addresses
    ":6380",                      // TCP listen address
)
srv.Serve()  // blocks
```

### Supported commands

| Command | Behaviour |
|---------|-----------|
| `SET key value` | `c.Put(ctx, key, value)` |
| `GET key` | `c.Get(ctx, key)` → bulk string or null bulk |
| `DEL key` | `c.Delete(ctx, key)` → integer 1 |
| `PING` | `+PONG\r\n` |
| `COMMAND` | `+OK\r\n` — satisfies `redis-cli` startup handshake |
| `QUIT` | Closes connection |

### Command parsing

The server handles two RESP formats:

**Array format** (standard, used by all client libraries):
```
*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
```

**Inline format** (used by telnet, curl, manual testing):
```
GET foo\r\n
```

Both are parsed and dispatched identically.

### Connection model

Each TCP connection is handled in its own goroutine. The server accepts connections in a loop:

```go
for {
    conn, err := listener.Accept()
    go handleConn(conn)
}
```

There is no connection pool limit — OS TCP backlog is the only throttle. For a production system, you'd add a semaphore on active connections.

### RESP response helpers

| Helper | Output | Used for |
|--------|--------|----------|
| `writeSimple("+OK")` | `+OK\r\n` | success responses |
| `writeError("ERR msg")` | `-ERR msg\r\n` | error responses |
| `writeBulk("value")` | `$5\r\nvalue\r\n` | GET response |
| `writeNull()` | `$-1\r\n` | GET miss |
| `writeInt(1)` | `:1\r\n` | DEL response |

### Testing with redis-cli

```bash
# basic operations
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
redis-cli -p 6380 DEL hello
redis-cli -p 6380 PING

# interactive mode
redis-cli -p 6380
127.0.0.1:6380> SET counter 0
OK
127.0.0.1:6380> GET counter
"0"

# load test (requires redis-benchmark)
redis-benchmark -p 6380 -t set,get -n 100000 -q
```

### Testing with Python

```python
import redis
r = redis.Redis(port=6380)
r.set('test', 'helios works')
assert r.get('test') == b'helios works'
print("ok")
```

### Testing with Node.js

```javascript
const { createClient } = require('redis');
const client = createClient({ socket: { port: 6380 } });
await client.connect();
await client.set('key', 'value');
const val = await client.get('key');
console.log(val);  // "value"
```
