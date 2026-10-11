#!/usr/bin/env bash
# Chaos test entry point. The original spec asked for "a shell script that
# runs 50 iterations" -- the actual orchestration (spawning a child JVM,
# killing it with SIGKILL at a random moment, verifying every acknowledged
# write survived, repeating) lives in dev.helios.Main's `chaos` mode
# instead of in bash. Reasons: ProcessBuilder gives real control over the
# child's stdout stream and exit handling, Process.destroyForcibly() is a
# portable SIGKILL across platforms where `kill -9` syntax isn't, and the
# verification step needs to run HeliosDB directly (not shell out to yet
# another JVM just to check values). This script is the documented,
# spec-shaped entry point; Main.chaos() is where the real logic is.
set -euo pipefail

cd "$(dirname "$0")/.."

DATA_DIR="${1:-/tmp/helios-chaos}"
ITERATIONS="${2:-50}"

if [ ! -d out ] || [ -z "$(find out -name '*.class' -print -quit 2>/dev/null)" ]; then
    echo "Compiling helios-kv..."
    mkdir -p out
    find src -name "*.java" > /tmp/helios-chaos-sources.txt
    javac -d out @/tmp/helios-chaos-sources.txt
fi

echo "Chaos test: $ITERATIONS iterations against $DATA_DIR"
echo "Each iteration: spawn a writer, SIGKILL it at a random moment, verify every acknowledged write survived."
echo

rm -rf "$DATA_DIR"
java -cp out dev.helios.Main chaos "$DATA_DIR" "$ITERATIONS"
EXIT_CODE=$?

rm -rf "$DATA_DIR"
exit $EXIT_CODE
