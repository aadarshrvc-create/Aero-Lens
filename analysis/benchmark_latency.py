#!/usr/bin/env python3
"""Compare parser CPU throughput on a synthetic cluster workload.

Each logical node reuses one generated log, so this measures warm-cache parser
throughput, not the disk I/O or end-to-end latency of a production deployment.
An optional baseline tool must produce byte-for-byte identical output.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

HISTOGRAMS = ['batch-index', 'batch-rec-count', '{test}-batch-sub-read', '{test}-batch-sub-write']


def generate_log(path, snapshots):
    start = datetime.datetime(2026, 11, 23)
    with path.open('w') as out:
        for i in range(snapshots):
            stamp = (start + datetime.timedelta(seconds=i * 10)).strftime('%b %d %Y %H:%M:%S')
            for hist in HISTOGRAMS:
                prefix = f'{stamp} GMT: INFO (info): (hist.c:1)'
                out.write(f'{prefix} histogram dump: {hist} ({(i + 1) * 1000} total) msec\n')
                out.write(f'{prefix} (00: {(i + 1) * 900:010d}) (01: {(i + 1) * 80:010d}) (04: {(i + 1) * 20:010d})\n')
            for j in range(20):
                out.write(f'{stamp} GMT: INFO (ticker): (ticker.c:100) {{test}} synthetic operational context record {j:02d}\n')


def measure(tool, log, nodes, workers):
    def run(hist):
        proc = subprocess.run([sys.executable, str(tool), '-l', str(log), '-h', hist,
                               '-f', 'head', '-t', '10', '-n', '6', '-e', '1'],
                              capture_output=True, timeout=900)
        if proc.returncode:
            raise RuntimeError(proc.stderr.decode(errors='replace') or proc.stdout.decode(errors='replace'))
        return hist, hashlib.sha256(proc.stdout + proc.stderr).hexdigest()

    started = time.perf_counter()
    hashes = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for hist, digest in pool.map(run, HISTOGRAMS * nodes):
            if hist in hashes and hashes[hist] != digest:
                raise RuntimeError(f'Inconsistent output for {hist}')
            hashes[hist] = digest
    return {'seconds': round(time.perf_counter() - started, 3), 'sha256_by_histogram': hashes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tool', type=Path, default=Path(__file__).resolve().parents[1] / 'asloglatency' / 'log_latency.py')
    parser.add_argument('--baseline-tool', type=Path)
    parser.add_argument('--nodes', type=int, default=30)
    parser.add_argument('--snapshots', type=int, default=4875)
    parser.add_argument('--workers', type=int, default=min(6, os.cpu_count() or 2))
    args = parser.parse_args()
    if args.nodes < 1 or args.snapshots < 2 or args.workers < 1:
        parser.error('nodes and workers must be positive; snapshots must be at least 2')
    with tempfile.TemporaryDirectory(prefix='aero-latency-benchmark-') as folder:
        log = Path(folder) / 'representative.log'
        generate_log(log, args.snapshots)
        print(json.dumps({'nodes': args.nodes, 'histograms_per_node': len(HISTOGRAMS),
                          'log_bytes_per_node': log.stat().st_size,
                          'histogram_dumps_per_node': args.snapshots * len(HISTOGRAMS),
                          'workers': args.workers}), flush=True)
        baseline = None
        if args.baseline_tool:
            baseline = measure(args.baseline_tool, log, args.nodes, args.workers)
            print(json.dumps({'baseline': baseline}), flush=True)
        optimized = measure(args.tool, log, args.nodes, args.workers)
        print(json.dumps({'current': optimized}), flush=True)
        if baseline:
            if baseline['sha256_by_histogram'] != optimized['sha256_by_histogram']:
                raise RuntimeError('Baseline and current parser outputs differ')
            print(json.dumps({'speedup': round(baseline['seconds'] / optimized['seconds'], 2),
                              'outputs_identical': True}), flush=True)


if __name__ == '__main__':
    main()
