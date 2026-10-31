// applylat measures per-entry apply latency through the real Go bridge against
// a single helios-kv JVM, with no raft in the loop. It exists to separate
// "what does one apply cost" from everything raft/compaction does around it.
//
//	go run ./research/cmd/applylat -addr 127.0.0.1:18990 -n 400
package main

import (
	"flag"
	"fmt"
	"sort"

	"github.com/manoj-1407/helios/raft"
	"github.com/manoj-1407/helios/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18990", "helios-kv address")
	n := flag.Int("n", 400, "number of sequential applies")
	keys := flag.Int("keys", 200, "distinct keys cycled through (the bench uses 200)")
	flag.Parse()

	b := store.NewHeliosKVBridge(*addr)
	for i := 0; i < *n; i++ {
		cmd := store.EncodeCommand(store.Command{
			Type:  store.CmdPut,
			Key:   fmt.Sprintf("key%d", i%*keys),
			Value: fmt.Sprintf("v-%d", i),
		})
		if err := b.Apply(raft.LogEntry{Term: 1, Index: uint64(i + 1), Command: cmd}); err != nil {
			fmt.Println("apply error:", err)
			return
		}
	}
	pts := b.Metrics.Snapshot()
	lat := make([]int64, 0, len(pts))
	for _, p := range pts {
		lat = append(lat, p.ReplicationLagNs)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p int) float64 { return float64(lat[(len(lat)-1)*p/100]) / 1e6 }
	fmt.Printf("n=%d  put latency ms: p50=%.2f p90=%.2f p99=%.2f max=%.2f\n",
		len(lat), pct(50), pct(90), pct(99), float64(lat[len(lat)-1])/1e6)
}
