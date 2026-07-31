#!/usr/bin/env bash
#
# Round 1 of the Aug-25 gap-narrowing search: a +/- delta probe on each primary Prophet knob
# about the locked s01 desktop config, for a central-difference gradient and curvature.
#
# One knob moves per run; every other knob holds its s01 value. The s01 point itself is NOT
# re-run -- data-official/2026-08/desktop_locked/ already provides the center of each difference.
#
# All runs symlink the shared raw BigQuery pull via --raw-cache-dir, so no BQ query is issued
# and every candidate trains on byte-identical input.
#
# Usage:  research/param-scans/aug25-gap/run_gradient_round1.sh [max_concurrent]
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$REPO_ROOT"

MAX_CONCURRENT="${1:-3}"
FORECAST_START="2026-07-28"
RESULTS_DIR="research/param-scans/aug25-gap/runs"
LOG_DIR="research/param-scans/aug25-gap/logs"
RAW_CACHE_DIR="data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825"

# s01 locked config -- the center point of every central difference.
S01_CPS=0.1849
S01_CPR=0.734
S01_NCP=35
S01_RECENT=17
S01_SPS=0.00825

mkdir -p "$RESULTS_DIR" "$LOG_DIR"

if [[ ! -d "$RAW_CACHE_DIR" ]]; then
    echo "FATAL: raw cache dir missing: $RAW_CACHE_DIR" >&2
    echo "Without it every run would re-query BigQuery and train on a different pull." >&2
    exit 1
fi

# Each row: label cps cpr ncp recent sps
# Deltas: cps +/-0.02, cpr +/-0.05, ncp +/-5, recent +/-3, sps +/-0.00175
JOBS=(
    "cps_lo    0.1649 $S01_CPR $S01_NCP $S01_RECENT $S01_SPS"
    "cps_hi    0.2049 $S01_CPR $S01_NCP $S01_RECENT $S01_SPS"
    "cpr_lo    $S01_CPS 0.684  $S01_NCP $S01_RECENT $S01_SPS"
    "cpr_hi    $S01_CPS 0.784  $S01_NCP $S01_RECENT $S01_SPS"
    "ncp_lo    $S01_CPS $S01_CPR 30     $S01_RECENT $S01_SPS"
    "ncp_hi    $S01_CPS $S01_CPR 40     $S01_RECENT $S01_SPS"
    "recent_lo $S01_CPS $S01_CPR $S01_NCP 14        $S01_SPS"
    "recent_hi $S01_CPS $S01_CPR $S01_NCP 20        $S01_SPS"
    "sps_lo    $S01_CPS $S01_CPR $S01_NCP $S01_RECENT 0.0065"
    "sps_hi    $S01_CPS $S01_CPR $S01_NCP $S01_RECENT 0.01"
)

run_one() {
    local label="$1" cps="$2" cpr="$3" ncp="$4" recent="$5" sps="$6"
    local log="$LOG_DIR/${label}.log"
    local started
    started=$(date +%s)

    echo "[$(date +%H:%M:%S)] START  $label  cps=$cps cpr=$cpr ncp=$ncp recent=$recent sps=$sps"

    # Holiday knobs are pinned to package defaults on every run: standing policy is that strictly
    # local effects must never be used to move a whole-season quantity. Regime is pinned to s01's
    # multiplicative so the gradient stays within one seasonality regime.
    if python scripts/run_param_scan.py \
            --forecast-start-date "$FORECAST_START" \
            --results-dir "$RESULTS_DIR" \
            --raw-cache-dir "$RAW_CACHE_DIR" \
            --changepoint-prior-scale "$cps" \
            --changepoint-range "$cpr" \
            --n-changepoints "$ncp" \
            --recent-weeks "$recent" \
            --seasonality-prior-scale "$sps" \
            --seasonality-regime multiplicative \
            --holiday-threshold -0.032 \
            --holiday-max-radius 5 \
            --holiday-min-radius 3 \
            --holiday-effect-floor -0.6 \
            > "$log" 2>&1; then
        echo "[$(date +%H:%M:%S)] DONE   $label  ($(( ($(date +%s) - started) / 60 ))m $(( ($(date +%s) - started) % 60 ))s)"
    else
        echo "[$(date +%H:%M:%S)] FAILED $label  ($(( $(date +%s) - started ))s) -- see $log"
        tail -20 "$log"
        return 1
    fi
}

echo "Round 1 gradient probe: ${#JOBS[@]} runs, up to $MAX_CONCURRENT concurrent."
echo "Raw cache: $RAW_CACHE_DIR"
echo

# Track PIDs explicitly and wait on each exactly once. Throttling on `jobs -rp` instead
# spins: once every job is reaped, `jobs -rp` can still report a finished-but-unreaped job,
# so the loop re-enters and `wait -n` returns 127 immediately -- inflating the failure count
# by tens of thousands while every run actually succeeded.
failures=0
pids=()
for job in "${JOBS[@]}"; do
    while (( ${#pids[@]} >= MAX_CONCURRENT )); do
        wait "${pids[0]}" || failures=$((failures + 1))
        pids=("${pids[@]:1}")
    done
    # shellcheck disable=SC2086
    run_one $job &
    pids+=("$!")
done

for pid in "${pids[@]}"; do
    wait "$pid" || failures=$((failures + 1))
done

echo
echo "All runs finished. Failures: $failures"
exit $(( failures > 0 ? 1 : 0 ))
