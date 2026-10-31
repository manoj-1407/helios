// calib drives one helios-kv JVM directly over HTTP (no raft) with a chosen key
// space, value size and write volume, then reports what the engine actually did:
// flushes, compactions, bytes, busy time, sstables. It exists to find a workload
// that compacts for real before any replication experiment is run on it.
//
//	go run ./research/cmd/calib -addr 127.0.0.1:18990 -keys 100000 -valsize 1000 -n 100000
//
// Output is one CSV row on stdout (header with -header). Progress goes to stderr.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type stats struct {
	WriteAmp        float64 `json:"write_amplification"`
	Sstables        int     `json:"sstable_count"`
	Flushes         int64   `json:"flushes"`
	Compactions     int64   `json:"compactions"`
	TablesCompacted int64   `json:"tables_compacted"`
	BusyMs          int64   `json:"compaction_busy_ms"`
	LogicalBytes    int64   `json:"logical_bytes"`
	FlushBytes      int64   `json:"flush_bytes"`
	CompactionBytes int64   `json:"compaction_bytes"`
}

func fetch(c *http.Client, addr string) (stats, error) {
	var s stats
	resp, err := c.Get("http://" + addr + "/stats")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:18990", "helios-kv address")
	keys := flag.Int("keys", 100000, "distinct keys (uniform)")
	valsize := flag.Int("valsize", 1000, "value size in bytes")
	n := flag.Int("n", 100000, "number of puts")
	conc := flag.Int("conc", 4, "concurrent writers")
	seed := flag.Int64("seed", 1, "workload seed")
	label := flag.String("label", "", "free-form label copied to the csv (mode, memtable, ...)")
	verify := flag.Bool("verify", false, "use unique keys, then read every acknowledged put back and count missing or wrong values")
	header := flag.Bool("header", false, "print csv header first")
	flag.Parse()

	c := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16}}
	if *header {
		fmt.Println("label,keys,valsize,n,conc,seed,put_secs,puts_per_s,flushes,compactions,tables_compacted,busy_ms,sstables,logical_bytes,flush_bytes,compaction_bytes,write_amp_reported,write_amp_flush_only")
	}
	base, err := fetch(c, *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stats:", err)
		os.Exit(1)
	}

	val := strings.Repeat("x", *valsize)
	var acked sync.Map // key -> value, only for 200 responses (verify mode)
	var wg sync.WaitGroup
	var failed int64
	var firstErr string
	var mu sync.Mutex
	start := time.Now()
	per := *n / *conc
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(*seed*1000 + int64(w)))
			for i := 0; i < per; i++ {
				k := r.Intn(*keys)
				v := val
				if *verify {
					k = w*per + i
					v = fmt.Sprintf("%08d-%s", k, val)[:max(*valsize, 9)]
				}
				body := fmt.Sprintf("key%08d\n%s", k, v)
				resp, err := c.Post("http://"+*addr+"/put", "text/plain", bytes.NewBufferString(body))
				if err != nil {
					mu.Lock()
					failed++
					if firstErr == "" {
						firstErr = fmt.Sprintf("after %.1fs: %v", time.Since(start).Seconds(), err)
					}
					mu.Unlock()
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 && *verify {
					acked.Store(fmt.Sprintf("key%08d", k), v)
				}
				if resp.StatusCode != 200 {
					mu.Lock()
					failed++
					if firstErr == "" {
						firstErr = fmt.Sprintf("after %.1fs: status %d", time.Since(start).Seconds(), resp.StatusCode)
					}
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	putSecs := time.Since(start).Seconds()

	// wait until compaction activity is quiet for 6s (3 ticks) or 90s pass
	last, _ := fetch(c, *addr)
	quiet := time.Now()
	for time.Since(start) < time.Duration(putSecs*float64(time.Second))+90*time.Second {
		time.Sleep(1 * time.Second)
		s, err := fetch(c, *addr)
		if err != nil {
			break
		}
		if s.Compactions != last.Compactions || s.Flushes != last.Flushes {
			quiet = time.Now()
		}
		last = s
		if time.Since(quiet) > 6*time.Second {
			break
		}
	}
	s := last
	if *verify {
		missing, wrong, total := 0, 0, 0
		acked.Range(func(k, v any) bool {
			total++
			resp, err := c.Post("http://"+*addr+"/get", "text/plain", bytes.NewBufferString(k.(string)))
			if err != nil {
				missing++
				return true
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 404 {
				missing++
			} else if string(b) != v.(string) {
				wrong++
			}
			return true
		})
		fmt.Fprintf(os.Stderr, "VERIFY: acknowledged=%d missing=%d wrong_value=%d failed_puts=%d\n", total, missing, wrong, failed)
	}
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: %d failed puts; first: %s\n", failed, firstErr)
	}
	flushOnly := 0.0
	if s.LogicalBytes-base.LogicalBytes > 0 {
		flushOnly = float64(s.FlushBytes-base.FlushBytes) / float64(s.LogicalBytes-base.LogicalBytes)
	}
	fmt.Printf("%s,%d,%d,%d,%d,%d,%.2f,%.0f,%d,%d,%d,%d,%d,%d,%d,%d,%.4f,%.4f\n",
		*label, *keys, *valsize, per**conc, *conc, *seed, putSecs, float64(per**conc)/putSecs,
		s.Flushes-base.Flushes, s.Compactions-base.Compactions, s.TablesCompacted-base.TablesCompacted,
		s.BusyMs-base.BusyMs, s.Sstables, s.LogicalBytes-base.LogicalBytes, s.FlushBytes-base.FlushBytes,
		s.CompactionBytes-base.CompactionBytes, s.WriteAmp, flushOnly)
}
