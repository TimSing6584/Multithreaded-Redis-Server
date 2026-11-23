#!/usr/bin/env bash
# Benchmarks Redis-k02 single-thread vs multithread with memtier_benchmark and writes the
# results to result.md, next to this script. Both servers listen on port 4000.
# Scenarios:
#   light  - 50 clients,                    80% GET / 20% SET on a prefilled keyspace
#   high   - 200 clients over 4 threads,    80% GET / 20% SET on a prefilled keyspace
#   heavy  - 200 clients over 4 threads,    SMEMBERS on ~400-member sets
# Only one server runs at a time, each run on a freshly started server. Every scenario runs
# ROUNDS rounds of single -> multi, so for light:
#   single, multi,  single, multi,  single, multi
# then the same for high and heavy. Identical runs can differ by 10-30% on a laptop, so the
# report shows, per (scenario, server), the run with the median throughput; taking turns
# means a slow stretch of the machine hits both servers alike.
# The script builds Redis-k02 and starts/stops every server itself, so port 4000 must be
# free before you run it.
#
# Usage:
#   run.sh                      all runs, then writes result.md
#   run.sh <scenario> [server]  only those runs, printed to the terminal; result.md is untouched
#                               scenario: light | high | heavy,  server: single | multi
#   ROUNDS=1 run.sh ...         one round instead of 3
#
# Redis-k02 is Linux only (epoll), so run it inside WSL:
#   wsl -d Ubuntu bash -lc 'bash /mnt/f/Git/Redis-k02/benchmark/run.sh'
# Needs go, redis-cli, memtier_benchmark, python3 and ss. Takes about 6 minutes.
# memtier_stats.py, next to this script, turns each memtier report into one CSV row.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)" # module root, where go.mod is
CONFIG="$ROOT/internal/config/config.go"
OUT="$HERE/result.md"
PORT=4000 # must match config.Port
ALL_SCENARIOS=(light high heavy)
ALL_VERSIONS=(single multi)
ROUNDS=${ROUNDS:-3} # runs per (scenario, server); the median-throughput one is reported

KEYS=100000  # string keys for light/high, all prefilled
SETS=1000    # sets for heavy
MEMBERS=400  # members per set for heavy
DATA_SIZE=32 # SET value size in bytes

TMP="$(mktemp -d)"
BIN="$TMP/redis-k02"
PID="" # pid of the server currently running

die() {
	echo "error: $*" >&2
	exit 1
}

start_server() { # start_server <single|multi>
	local v=$1
	case $v in
	single) "$BIN" <<<1 >"$TMP/$v.log" 2>&1 & ;; # <<<1 answers the "which version" prompt
	multi) "$BIN" <<<2 >"$TMP/$v.log" 2>&1 & ;;
	esac
	PID=$!
	for _ in $(seq 50); do
		redis-cli -p "$PORT" PING >/dev/null 2>&1 && return 0
		sleep 0.1
	done
	cat "$TMP/$v.log" >&2
	die "$v server did not start on port $PORT"
}

stop_server() { # SIGTERM = graceful shutdown
	[ -n "$PID" ] || return 0
	kill -TERM "$PID" 2>/dev/null || true
	for _ in $(seq 50); do
		kill -0 "$PID" 2>/dev/null || break
		sleep 0.1
	done
	kill -KILL "$PID" 2>/dev/null || true
	wait "$PID" 2>/dev/null || true
	PID=""
}

cleanup() {
	stop_server
	rm -rf "$TMP"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# memtier <args...>: memtier_benchmark against 127.0.0.1:PORT, progress output hidden.
# --distinct-client-seed: without it every client walks the same random key sequence.
memtier() {
	memtier_benchmark -s 127.0.0.1 -p "$PORT" --hide-histogram --distinct-client-seed "$@" \
		>"$TMP/memtier.log" 2>&1 || {
		tail -n 20 "$TMP/memtier.log" >&2
		die "memtier_benchmark failed: $*"
	}
}

# --- scenarios: each has a prefill (not measured) and the memtier args of the measured run ---

# Writes every key once, so the measured GETs read real values instead of missing keys.
prefill_strings() {
	memtier -t 4 -c 25 --ratio=1:0 --key-pattern=P:P --key-minimum=1 --key-maximum="$KEYS" \
		-n allkeys -d "$DATA_SIZE"
}

# SETS sets "set:0".."set:N-1", each with exactly MEMBERS members "m:000000000000".., so an
# SMEMBERS reply is ~8 KB. One SADD per set, sent by redis-cli reading commands from stdin.
# (Not redis-cli --pipe: it ends with ECHO, which Redis-k02 doesn't implement.)
prefill_sets() {
	awk -v sets="$SETS" -v members="$MEMBERS" 'BEGIN {
		for (i = 0; i < sets; i++) {
			line = "SADD set:" i
			for (j = 0; j < members; j++) line = line sprintf(" m:%012d", j)
			print line
		}
	}' | redis-cli -p "$PORT" >/dev/null
	local n
	n=$(redis-cli -p "$PORT" SMEMBERS "set:$((SETS - 1))" | wc -l)
	[ "$n" -eq "$MEMBERS" ] || die "set prefill failed: set:$((SETS - 1)) has $n members, want $MEMBERS"
}

MIXED=(--ratio=1:4 --key-pattern=R:R --key-minimum=1 --key-maximum="$KEYS" -d "$DATA_SIZE")
HEAVY=(--command="SMEMBERS __key__" --command-key-pattern=R --key-prefix=set:
	--key-minimum=0 --key-maximum=$((SETS - 1)))

commas() { echo "$1" | sed -E ':a; s/([0-9])([0-9]{3})($|,)/\1,\2\3/; ta'; } # 100000 -> 100,000

declare -A TITLE DESC PREFILL ARGS
TITLE[light]='Light workload: 50 clients'
DESC[light]="1,000,000 requests, **80% GET / 20% SET** mixed, from **50 clients** (1 client thread, 20,000 requests each), on random keys out of $(commas $KEYS). All keys are written once beforehand, so GETs hit. Values are $DATA_SIZE bytes."
PREFILL[light]=prefill_strings
ARGS[light]="-t 1 -c 50 -n 20000"

TITLE[high]='High concurrency: 200 clients'
DESC[high]="2,000,000 requests, **80% GET / 20% SET** mixed, from **200 clients** over 4 client threads (10,000 requests each), on random keys out of $(commas $KEYS). All keys are written once beforehand, so GETs hit. Values are $DATA_SIZE bytes."
PREFILL[high]=prefill_strings
ARGS[high]="-t 4 -c 50 -n 10000"

TITLE[heavy]='Heavy command: SMEMBERS on ~400-member sets'
DESC[heavy]="Prefill: $(commas $SETS) sets with $MEMBERS members each. Then 200,000 SMEMBERS from **200 clients** over 4 client threads (1,000 requests each), on random sets. Each reply is ~8 KB, so every request costs the server real CPU."
PREFILL[heavy]=prefill_sets
ARGS[heavy]="-t 4 -c 50 -n 1000"

args_of() { # args_of <scenario>: the full memtier args of its measured run, one per line
	local s=$1
	# shellcheck disable=SC2206
	local a=(${ARGS[$s]})
	if [ "$s" = heavy ]; then a+=("${HEAVY[@]}"); else a+=("${MIXED[@]}"); fi
	printf '%s\n' "${a[@]}"
}

# measure <csv-out> <memtier args...>: runs memtier once and writes one CSV row
# (see memtier_stats.py, which also explains why latencies come from memtier's histogram):
#   ops/sec,mean,p50,p99,p99.9,left_out,get_hits/sec,get_misses/sec   (latencies in ms)
measure() {
	local csv=$1 json="$TMP/run.json" row
	shift
	for _ in 1 2; do
		rm -f "$json"
		memtier --json-out-file="$json" "$@"
		row=$(python3 "$HERE/memtier_stats.py" "$json")
		# The wall clock in WSL2 sometimes steps, which can make ops/sec nonsense: rerun once.
		awk -F, '{ exit !($1 <= 0) }' <<<"$row" || break
		echo "   got ops/sec <= 0, running it again" >&2
	done
	echo "$row" >"$csv"
	awk -F, '$7 + $8 > 0 && $8 / ($7 + $8) > 0.01 {
		printf "   warning: %.1f%% of GETs missed - was the keyspace prefilled?\n", 100 * $8 / ($7 + $8)
	}' "$csv" >&2
}

# run <scenario> <server> <round>: one run on a fresh server; the result lands in
# $TMP/<scenario>-<server>-<round>.csv
run() {
	local s=$1 v=$2 r=$3 args
	mapfile -t args < <(args_of "$s")
	echo ">> $s / $v, run $r of $ROUNDS" >&2
	start_server "$v"
	"${PREFILL[$s]}"
	measure "$TMP/$s-$v-$r.csv" "${args[@]}"
	stop_server
	awk -F, '{ printf "   %.0f ops/sec, mean %s / p50 %s / p99 %s / p99.9 %s ms%s\n", $1, $2, $3, $4, $5,
		($6 > 0 ? " (left out " $6 " impossible samples)" : "") }' "$TMP/$s-$v-$r.csv" >&2
}

# pick_median <scenario> <server>: copies the run with the median throughput (all of its
# numbers, so they come from one run) to $TMP/<scenario>-<server>.csv, and lists every run's
# throughput, in run order, in $TMP/<scenario>-<server>.all.
pick_median() {
	local s=$1 v=$2 r
	for r in $(seq "$ROUNDS"); do cat "$TMP/$s-$v-$r.csv"; done >"$TMP/$s-$v.runs"
	sort -t, -k1,1g "$TMP/$s-$v.runs" | sed -n "$(((ROUNDS + 1) / 2))p" >"$TMP/$s-$v.csv"
	cut -d, -f1 "$TMP/$s-$v.runs" >"$TMP/$s-$v.all"
	awk -F, -v s="$s" -v v="$v" '{ printf ">> %s / %s median: %.0f ops/sec, mean %s / p50 %s / p99 %s ms\n", s, v, $1, $2, $3, $4 }' \
		"$TMP/$s-$v.csv" >&2
}

# --- report ---

# commas formats a number with thousands separators; used by the awk programs below.
AWK_LIB='
function commas(x,   s, r) {
	s = sprintf("%.0f", x); r = ""
	while (length(s) > 3) { r = "," substr(s, length(s) - 2) r; s = substr(s, 1, length(s) - 3) }
	return s r
}'

# table <scenario>: the result.md table, one row per server; the better value of each column is bold.
table() {
	local s=$1
	awk -F, "$AWK_LIB"'
		{ for (c = 1; c <= 5; c++) val[FILENAME, c] = $c; files[++n] = FILENAME }
		END {
			label[1] = "Single-thread"; label[2] = "Multithread"
			print "| | Throughput (ops/sec) | Mean latency (ms) | p50 latency (ms) | p99 latency (ms) | p99.9 latency (ms) |"
			print "|---|---:|---:|---:|---:|---:|"
			for (i = 1; i <= 2; i++) {
				row = "| " label[i] " |"
				for (c = 1; c <= 5; c++) {
					x = val[files[i], c]; y = val[files[3 - i], c] # y: the other server
					s = (c == 1) ? commas(x) : x
					better = (c == 1) ? x + 0 > y + 0 : x + 0 < y + 0
					row = row " " (better ? "**" s "**" : s) " |"
				}
				print row
			}
		}' "$TMP/$s-single.csv" "$TMP/$s-multi.csv"
}

# readme_tables: the two tables for README section E.1 (throughput; mean / p50 / p99 latency).
readme_tables() {
	local s v
	for s in "${ALL_SCENARIOS[@]}"; do
		for v in "${ALL_VERSIONS[@]}"; do
			printf '%s,%s,' "$s" "$v"
			cat "$TMP/$s-$v.csv"
		done
	done | awk -F, '
		{ ops[$1, $2] = $3; lat[$1, $2] = $4 " / " $5 " / " $6 }
		END {
			n = split("light high heavy", sc, " "); split("single multi", sv, " ")
			name["light"] = "1. Light workload (50 clients)"
			name["high"] = "2. High concurrency (200 clients, 4 client threads)"
			name["heavy"] = "3. Heavy command (SMEMBERS on ~400-member sets, 200 clients, 4 client threads)"
			head = "| Scenario | Single-thread | Multithread |\n|---|---:|---:|"
			print "+) **Throughput (ops/sec, higher is better)**\n\n" head
			for (i = 1; i <= n; i++) {
				row = "| " name[sc[i]] " |"
				for (j = 1; j <= 2; j++) {
					x = ops[sc[i], sv[j]]
					row = row " " (x >= 10000 ? sprintf("%.0fk", x / 1000) : sprintf("%.1fk", x / 1000)) " |"
				}
				print row
			}
			print "\n+) **Latency (mean / p50 / p99 in ms, lower is better)**\n\n" head
			for (i = 1; i <= n; i++) {
				row = "| " name[sc[i]] " |"
				for (j = 1; j <= 2; j++) row = row " " lat[sc[i], sv[j]] " |"
				print row
			}
		}'
}

# all_runs <scenario> <server>: every run's throughput, e.g. "138,984 / 122,344 / 131,007"
all_runs() {
	awk "$AWK_LIB"'{ printf "%s%s", (NR > 1 ? " / " : ""), commas($1) }' "$TMP/$1-$2.all"
}

write_report() {
	local nm cpu memtier_version s args
	nm=$(grep -oE 'io handlers=[0-9]+, workers=[0-9]+' "$TMP/multi.log" | head -n 1 || true)
	cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ *//')
	memtier_version=$(memtier_benchmark --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)
	{
		cat <<EOF
# Benchmark: Redis-k02 single-thread vs multithread

Generated by \`benchmark/run.sh\` on $(date '+%Y-%m-%d %H:%M') (took $((SECONDS / 60)) min).

- **Machine:** $cpu, $(nproc) CPUs, $(uname -sr)
- **Load generator:** memtier_benchmark $memtier_version, on the same machine, so it competes with the server for CPU.
- **Redis-k02** on port $PORT: single-thread, and multithread (${nm:-io handlers/workers unknown}). MaxKeys = $MAX_KEYS.
- Every memtier command also has \`-s 127.0.0.1 --hide-histogram --distinct-client-seed\`, and the measured runs \`--json-out-file\`.
- Each scenario ran **$ROUNDS times per server**, alternating single-thread and multithread, one server at a time. The table shows the run with the **median throughput**; all its numbers (throughput and latency) come from that one run.
- Requests per run: **1M** for the light workload (50 clients × 20,000), **2M** for high concurrency (200 clients × 10,000), **200k** for the heavy command (200 clients × 1,000).
- Throughput is operations per second over the whole mix; latencies are in ms, over all requests of the run.
- Latencies are computed by \`benchmark/memtier_stats.py\` from memtier's own latency histogram rather than taken from its summary: on some runs memtier records impossible latencies, longer than the whole run (usually one per connection, sometimes two), which wrecks its mean. Those samples are left out; the note under each table says how many.
- **Bold** = the better of the two in each column (higher throughput, lower latency).

EOF
		local n=0
		for s in "${ALL_SCENARIOS[@]}"; do
			n=$((n + 1))
			mapfile -t args < <(args_of "$s")
			echo "## $n. ${TITLE[$s]}"
			echo
			echo "${DESC[$s]}"
			echo
			echo '```'
			if [ "$s" = heavy ]; then
				echo "redis-cli -p $PORT < sadd.txt   # prefill: $SETS lines \"SADD set:<i> m:000000000000 ... m:$(printf '%012d' $((MEMBERS - 1)))\""
			else
				echo "memtier_benchmark -p $PORT -t 4 -c 25 --ratio=1:0 --key-pattern=P:P --key-minimum=1 --key-maximum=$KEYS -n allkeys -d $DATA_SIZE   # prefill"
			fi
			printf 'memtier_benchmark -p %s' "$PORT"
			for a in "${args[@]}"; do
				if [[ $a == *' '* ]]; then printf ' %s="%s"' "${a%%=*}" "${a#*=}"; else printf ' %s' "$a"; fi
			done
			echo
			echo '```'
			echo
			table "$s"
			echo
			printf 'Throughput of every run, in run order (ops/sec): single-thread %s; multithread %s.\n\n' \
				"$(all_runs "$s" single)" "$(all_runs "$s" multi)"
			printf 'Impossible latency samples left out of the reported runs: single-thread %s, multithread %s.\n\n' \
				"$(cut -d, -f6 "$TMP/$s-single.csv")" "$(cut -d, -f6 "$TMP/$s-multi.csv")"
		done
	} >"$OUT"
	echo ">> Wrote $OUT" >&2
}

# --- main ---

SCENARIOS=("${ALL_SCENARIOS[@]}")
VERSIONS=("${ALL_VERSIONS[@]}")
if [ $# -ge 1 ]; then
	[[ " ${ALL_SCENARIOS[*]} " == *" $1 "* ]] || die "unknown scenario '$1' (want: ${ALL_SCENARIOS[*]})"
	SCENARIOS=("$1")
fi
if [ $# -ge 2 ]; then
	[[ " ${ALL_VERSIONS[*]} " == *" $2 "* ]] || die "unknown server '$2' (want: ${ALL_VERSIONS[*]})"
	VERSIONS=("$2")
fi
[ $# -le 2 ] || die "usage: run.sh [scenario] [server]"
[[ $ROUNDS =~ ^[1-9][0-9]*$ ]] || die "ROUNDS must be a positive number, got '$ROUNDS'"

for tool in go redis-cli memtier_benchmark python3 ss; do
	command -v "$tool" >/dev/null || die "$tool not found on PATH"
done
if ss -Hltn "( sport = :$PORT )" | grep -q .; then
	die "port $PORT is already in use - stop the server running there first (this script starts its own)"
fi
MAX_KEYS=$(grep -oE 'MaxKeys *= *[0-9]+' "$CONFIG" | grep -oE '[0-9]+$' || echo "?")
if [[ $MAX_KEYS =~ ^[0-9]+$ ]] && ((MAX_KEYS < 2 * KEYS)); then
	echo "warning: config.MaxKeys = $MAX_KEYS, but the tests use $KEYS keys, so Redis-k02 will" >&2
	echo "         evict keys during the benchmark. Set it to e.g. 10000000 while benchmarking." >&2
fi

echo ">> Building Redis-k02" >&2
(cd "$ROOT" && go build -o "$BIN" .)

for s in "${SCENARIOS[@]}"; do
	for r in $(seq "$ROUNDS"); do
		for v in "${VERSIONS[@]}"; do
			run "$s" "$v" "$r"
		done
	done
	for v in "${VERSIONS[@]}"; do
		pick_median "$s" "$v"
	done
done

if [ $# -eq 0 ]; then
	write_report
	echo >&2
	echo ">> README tables (section E.1):" >&2
	echo >&2
	readme_tables
fi
