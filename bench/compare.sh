#!/usr/bin/env bash
#
# Compare bench/results.json against bench/baseline.json.
#
# Exits 0 if every bench is at most THRESHOLD_PCT slower than baseline,
# 1 if any bench exceeds the threshold, 2 on missing/malformed inputs.
#
# Usage:
#   ./bench/compare.sh [threshold_pct] [results.json] [baseline.json]
#
# Default threshold is 20 (%). The results/baseline paths default to
# bench/results.json and bench/baseline.json. CI passes explicit paths so
# it can compare two runs measured on the same runner: the committed
# baseline is hardware-specific and not comparable across machines.

set -eu

THRESHOLD="${1:-20}"
HERE="$(cd "$(dirname "$0")" && pwd)"
RESULTS="${2:-$HERE/results.json}"
BASELINE="${3:-$HERE/baseline.json}"

for f in "$RESULTS" "$BASELINE"; do
    if [ ! -f "$f" ]; then
        echo "compare.sh: missing $f" >&2
        exit 2
    fi
done

if ! command -v jq >/dev/null 2>&1; then
    echo "compare.sh: jq is required" >&2
    exit 2
fi

REGRESSED=0
for name in registration events broadcast; do
    BASE=$(jq -r ".results.${name}.us_per_op" "$BASELINE")
    HAVE=$(jq -r ".results.${name}.us_per_op" "$RESULTS")
    if [ "$BASE" = "null" ] || [ "$HAVE" = "null" ]; then
        echo "compare.sh: missing field '${name}' in baseline or results" >&2
        exit 2
    fi
    DELTA=$(awk -v a="$HAVE" -v b="$BASE" 'BEGIN{printf "%.2f", (a-b)/b*100}')
    MARK="OK"
    BAD=$(awk -v d="$DELTA" -v t="$THRESHOLD" 'BEGIN{print (d > t) ? "1" : "0"}')
    if [ "$BAD" = "1" ]; then
        MARK="REGRESSION"
        REGRESSED=1
    fi
    printf "%-15s baseline=%8s us/op  current=%8s us/op  delta=%6s%%  %s\n" \
        "$name" "$BASE" "$HAVE" "$DELTA" "$MARK"
done

if [ "$REGRESSED" = "1" ]; then
    echo "compare.sh: at least one bench regressed >${THRESHOLD}%" >&2
    exit 1
fi
