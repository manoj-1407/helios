package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/manoj-1407/helios/bench"
)

func main() {
	ops        := flag.Int("ops",    50000,                   "operations per config")
	writeRatio := flag.Float64("wr", 0.9,                     "write ratio (0.0–1.0)")
	outJSON    := flag.String("out",  "bench/results/thesis.json", "path for JSON results file (empty to skip)")
	flag.Parse()

	fmt.Printf("helios replication benchmark — %d ops per config  write_ratio=%.0f%%\n",
		*ops, *writeRatio*100)
	fmt.Printf("started: %s\n\n", time.Now().Format("15:04:05"))

	results, err := bench.RunAll(*ops)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}

	bench.PrintSummary(results)
	fmt.Printf("\ncompleted: %s\n", time.Now().Format("15:04:05"))

	if *outJSON != "" {
		if err := bench.WriteJSON(results, *ops, *writeRatio, *outJSON); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: could not write JSON results: %v\n", err)
		} else {
			fmt.Printf("results written to: %s\n", *outJSON)
		}
	}
}
