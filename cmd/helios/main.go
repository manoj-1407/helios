package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/manoj-1407/helios/client"
	"github.com/manoj-1407/helios/dashboard"
	"github.com/manoj-1407/helios/raft"
	"github.com/manoj-1407/helios/store"
)

func main() {
	id        := flag.String("id",      "node0",                    "node ID (unique per cluster)")
	peers     := flag.String("peers",   "",                         "comma-separated peer IDs")
	dataDir   := flag.String("data",    "./data",                   "raft log + state directory")
	jvmAddr   := flag.String("jvm",     "127.0.0.1:18080",         "helios-kv JVM address")
	jvmJar    := flag.String("jar",     "helios-kv/helios-kv.jar", "path to helios-kv.jar")
	kvMode    := flag.String("mode",    "ADAPTIVE",                 "ADAPTIVE|FORCE_LEVELED|FORCE_TIERED")
	kvPort    := flag.String("kvport",  "18080",                    "helios-kv HTTP port")
	redisAddr := flag.String("redis",   ":7379",                    "redis-wire listen address")
	dashAddr  := flag.String("dash",    ":8080",                    "dashboard listen address")
	flag.Parse()

	fmt.Printf("helios node %s\n  jvm=%s mode=%s  redis=%s  dashboard=http://localhost%s\n\n",
		*id, *jvmAddr, *kvMode, *redisAddr, *dashAddr)

	// 1. start helios-kv JVM
	jvmData, _ := os.MkdirTemp("", "helios-kv-"+*id+"-*")
	jvmCmd := exec.Command("java", "-jar", *jvmJar, jvmData, *kvPort, *kvMode)
	jvmCmd.Stdout = os.Stdout
	jvmCmd.Stderr = os.Stderr
	if err := jvmCmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to start JVM:", err)
		os.Exit(1)
	}
	defer func() { jvmCmd.Process.Kill(); os.RemoveAll(jvmData) }()

	// 2. wait for JVM health
	bridge := store.NewHeliosKVBridge(*jvmAddr)
	fmt.Printf("waiting for JVM at %s...\n", *jvmAddr)
	for i := 0; i < 60; i++ {
		time.Sleep(500 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := bridge.Health(ctx)
		cancel()
		if err == nil {
			fmt.Println("JVM healthy")
			break
		}
		if i == 59 {
			fmt.Fprintln(os.Stderr, "JVM never became healthy")
			os.Exit(1)
		}
	}

	// 3. start Raft node
	peerList := []string{}
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			if p = strings.TrimSpace(p); p != "" {
				peerList = append(peerList, p)
			}
		}
	}
	transport := raft.NewChannelTransport()
	node, err := raft.NewRaftNode(*id, peerList, *dataDir, transport, bridge, raft.DefaultConfig())
	if err != nil {
		fmt.Fprintln(os.Stderr, "raft init:", err)
		os.Exit(1)
	}
	transport.Register(node)
	node.Start()
	defer node.Stop()
	fmt.Printf("raft node %s started (peers: %v)\n", *id, peerList)

	// 4. replication pressure loop — the feedback loop
	pressureLoop := store.NewCompactionPressureLoop(node, bridge)
	pressureLoop.Start()
	defer pressureLoop.Stop()
	fmt.Println("replication->compaction pressure loop active")

	// 5. redis-wire server
	go func() {
		rs, err := client.NewRESPServer([]string{*jvmAddr}, *redisAddr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "redis server:", err)
			return
		}
		if err := rs.Serve(); err != nil {
			fmt.Fprintln(os.Stderr, "redis server:", err)
		}
	}()
	fmt.Printf("redis-wire on %s — try: redis-cli -p %s set hello world\n",
		*redisAddr, strings.TrimPrefix(*redisAddr, ":"))

	// 6. web dashboard
	go func() {
		dash := dashboard.NewServer(
			[]*raft.RaftNode{node},
			[]*store.HeliosKVBridge{bridge},
			*dashAddr,
		)
		dash.Start()
	}()

	// 7. block until signal
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigC
	fmt.Printf("\n%v received, shutting down\n", sig)
}
