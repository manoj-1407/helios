package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/manoj-1407/helios/bench"
)

func main() {
	ops := flag.Int("ops", 50000, "operations per config")
	flag.Parse()

	fmt.Printf("helios replication benchmark — %d ops per config\n", *ops)
	fmt.Printf("started: %s\n\n", time.Now().Format("15:04:05"))

	results, err := bench.RunAll(*ops)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}

	bench.PrintSummary(results)
	fmt.Printf("\ncompleted: %s\n", time.Now().Format("15:04:05"))
}
