# Reproducibility

Everything in `research/results/` was produced by the commands below at commit `b700823` of this branch's parent, on one host. No result here is a claim about other hardware.

## Environment of the 2026-11-01 runs

| item | value |
|---|---|
| host | cloud container, 2 vCPU, 8 GiB RAM, ext4 on virtio block device |
| kernel | Linux 6.18.44-fc-v185 |
| Java | OpenJDK 21.0.12.1 (Ubuntu build) |
| Go | 1.24.7 linux/amd64 |
| git commit | `b7008238241043d35c4eef9a2e85f720cf728405` |
| seeds | the bench uses a fixed `rand.NewSource(42)` shared across goroutines (finding F15), so the workload is not reproducible even with the seed |
| other load | none deliberately; the container is shared infrastructure and I did not measure background load |

Two vCPUs for three JVMs, a Go process and the load generator is a heavy CPU confound. Treat absolute numbers as specific to this host. Treat only the within-host contrasts (44 ms versus 1 ms per put) as portable.

## Building the jar

Maven could not resolve its plugins from this host (plugin resolution error through the network allowlist), so the jar was built with the JDK tools:

```bash
cd helios-kv
mkdir -p /tmp/w/classes /tmp/w/helios-kv
javac -nowarn -d /tmp/w/classes $(find src/main/java -name '*.java')
jar --create --file /tmp/w/helios-kv/helios-kv.jar --main-class dev.helios.Main -C /tmp/w/classes .
```

This matches what `pom.xml` configures (`mainClass` = `dev.helios.Main`). The tracked `helios-kv/helios-kv.jar` was left untouched. It has a different `Main-Class` (finding F11) and cannot be launched by the bench.

## F3: per-put stall

```bash
java -cp helios-kv.jar dev.helios.Main server /tmp/d 18990 FORCE_LEVELED &
go run ./research/cmd/applylat -addr 127.0.0.1:18990 -n 400
# repeat with: java -Dsun.net.httpserver.nodelay=true -cp ...
```

Run three times per regime, each on a fresh data directory. Raw output: `results/applylat.txt`.

| regime | rep | p50 ms | p90 ms | p99 ms |
|---|---|---|---|---|
| default | 1 | 43.94 | 44.12 | 48.12 |
| default | 2 | 43.96 | 44.16 | 48.08 |
| default | 3 | 43.96 | 44.12 | 48.08 |
| nodelay | 1 | 0.99 | 3.02 | 7.39 |
| nodelay | 2 | 0.90 | 2.16 | 5.53 |
| nodelay | 3 | 0.97 | 2.44 | 6.37 |

## Order and config experiment

```bash
# from a directory that contains helios-kv/helios-kv.jar (the bench uses a relative path)
go build -o ordertest ./research/cmd/ordertest
JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS" ./ordertest -ops 4000 -reps 4 -regime default > ordertest_default.csv
JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -Dsun.net.httpserver.nodelay=true" ./ordertest -ops 4000 -reps 4 -regime nodelay > ordertest_nodelay.csv
```

22 runs per regime. The bench spawns JVMs with `cmd.Stdout = os.Stdout`, so JVM log lines land in the CSV. `results/clean_*.csv` are the same files filtered to rows beginning with the regime name. The raw files are kept unmodified.

## Limits

* 4 identical-config repetitions and 2 per config per position. That detects only large effects. The plan in `EXPERIMENT_DESIGN.md` calls for 10 or more.
* No confidence intervals are reported in this round; with n=4 and n=2 they would not be informative.
* No real compaction was verified in any run (F4, F6). These runs say nothing about compaction.
* Simulation and real hardware are not separated: everything is one machine, an in-process transport, shared CPU and one disk.
* The committed `bench/results/thesis.json` came from another machine and a differently built jar. I did not reproduce its absolute numbers and do not claim to explain the difference.
