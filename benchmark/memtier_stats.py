"""Summarizes one memtier_benchmark JSON report (--json-out-file) as a CSV row:

    ops/sec,mean,p50,p99,p99.9,left_out,get_hits,get_misses     (latencies in ms)

Why not just read memtier's own "Average Latency" and percentiles: on some runs memtier
records impossible latencies, about 72 minutes in a run of seconds, usually one per
connection and sometimes two (seen with memtier 2.6.1 on WSL2, against real Redis too; the
run is otherwise normal). One such sample per connection is enough to turn a 0.4 ms mean
into 200+ ms. So the latency stats are
recomputed here from memtier's own HdrHistogram of all requests, leaving out samples
longer than the run itself. On clean runs this gives the same numbers as memtier.

Usage: python3 memtier_stats.py report.json
"""
import base64
import json
import math
import struct
import sys
import zlib


def decode_histogram(b64):
    """Decodes an HdrHistogram V2 compressed log string into
    a list of ((lowest, highest) value of the bucket, count)."""
    raw = base64.b64decode(b64)
    cookie, length = struct.unpack(">ii", raw[:8])
    if cookie & ~0xF0 != 0x1C849304 & ~0xF0:
        raise ValueError("not a compressed HdrHistogram")
    data = zlib.decompress(raw[8 : 8 + length])
    _, payload_len, norm_offset, sig_digits, lowest, _, _ = struct.unpack(">iiiiqqd", data[:40])
    if norm_offset != 0:
        raise ValueError("normalized HdrHistogram is not supported")
    payload = data[40 : 40 + payload_len]

    # Bucket layout, as in HdrHistogram itself.
    unit_mag = int(math.floor(math.log2(lowest)))
    half_mag = max(int(math.ceil(math.log2(2 * 10**sig_digits))), 1) - 1
    half = 1 << half_mag

    def bucket_range(index):
        bucket = (index >> half_mag) - 1
        sub = (index & (half - 1)) + half
        if bucket < 0:
            sub -= half
            bucket = 0
        lo = sub << (bucket + unit_mag)
        return lo, lo + (1 << (bucket + unit_mag)) - 1

    # Counts are ZigZag LEB128 varints (up to 9 bytes); a negative one skips that many empty buckets.
    buckets, index, i = [], 0, 0
    while i < len(payload):
        v, shift = 0, 0
        for k in range(9):
            b = payload[i]
            i += 1
            if k == 8:
                v |= b << 56
                break
            v |= (b & 0x7F) << shift
            shift += 7
            if b < 0x80:
                break
        n = (v >> 1) ^ -(v & 1)
        if n < 0:
            index -= n
            continue
        if n:
            buckets.append((bucket_range(index), n))
        index += 1
    return buckets


def main():
    stats = json.load(open(sys.argv[1]))["ALL STATS"]
    totals = stats["Totals"]
    run_ms = stats["Runtime"]["Total duration"]
    histogram = totals["Percentile Latencies"]["Histogram log format"]["Compressed Histogram"]

    buckets = decode_histogram(histogram)  # values in microseconds
    kept = [(r, c) for r, c in buckets if r[0] / 1000 <= run_ms]
    left_out = sum(c for _, c in buckets) - sum(c for _, c in kept)
    total = sum(c for _, c in kept)
    mean = sum((lo + hi) / 2 * c for (lo, hi), c in kept) / total

    def percentile(p):  # same rank rule as HdrHistogram's hdr_value_at_percentile
        want = max(int(p / 100 * total + 0.5), 1)
        seen = 0
        for (_, hi), c in kept:
            seen += c
            if seen >= want:
                return hi
        return kept[-1][0][1]

    gets = stats.get("Gets", {})
    print(
        "%.2f,%.3f,%.3f,%.3f,%.3f,%d,%.2f,%.2f"
        % (
            totals["Ops/sec"],
            mean / 1000,
            percentile(50) / 1000,
            percentile(99) / 1000,
            percentile(99.9) / 1000,
            left_out,
            gets.get("Hits/sec", 0),
            gets.get("Misses/sec", 0),
        )
    )


if __name__ == "__main__":
    main()
