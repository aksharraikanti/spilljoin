"""Dependency-free, verified demo with a fresh process for each measurement."""

import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import platform
import resource
import subprocess
import sys
from tempfile import TemporaryDirectory

from .join import join_csv


CASES = ('memory', 'forced', 'skew')


def run_case(case, rows):
    """Measure the join, then compare the complete output multiset to an oracle."""
    with TemporaryDirectory(prefix='spilljoin-demo-') as directory:
        root = Path(directory)
        left = [('hot' if case == 'skew' else str(i), 'left,' + str(i))
                for i in range(rows)]
        right = [('hot' if case == 'skew' else str(i), 'right payload\n' + str(i))
                 for i in range(rows)]
        for name, header, values in (('left', ('id', 'value'), left),
                                      ('right', ('code', 'payload'), right)):
            with (root / (name + '.csv')).open('w', encoding='utf-8', newline='') as stream:
                csv.writer(stream).writerows([header, *values])
        metrics = {}
        limit = 1500 if case == 'skew' else 8 * 1024 * 1024
        join_csv(root / 'left.csv', root / 'right.csv', root / 'joined.csv', 'id', 'code',
                 force_spill=case == 'forced', build_limit=limit,
                 scratch_dir=root, metrics=metrics)
        # Sample before allocating the oracle or reading the output into memory.
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = int(rss if sys.platform == 'darwin' else rss * 1024)
        with (root / 'joined.csv').open(encoding='utf-8', newline='') as stream:
            reader = csv.reader(stream)
            header = next(reader)
            actual = Counter(map(tuple, reader))
        expected = Counter(a + b for a in left for b in right if a[0] == b[0])
        if (header != ['left.id', 'left.value', 'right.code', 'right.payload']
                or actual != expected or metrics['output_rows'] != sum(expected.values())):
            raise RuntimeError('demo output differs from complete reference join')
        if list(root.glob('.spilljoin-*')):
            raise RuntimeError('demo left temporary files behind')
        if case == 'skew' and metrics['fallback_partitions'] < 1:
            raise RuntimeError('skew fixture did not exercise the fallback')
        return {'case': case, 'rows_per_input': rows, 'build_limit_bytes': limit,
                'peak_rss_bytes': rss_bytes, 'verified': True, 'metrics': metrics}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=64,
                        help='rows per input, between 8 and 1000 (default: 64)')
    parser.add_argument('--repeats', type=int, default=1,
                        help='fresh workers per case, between 1 and 10')
    parser.add_argument('--worker', choices=CASES, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 8 <= args.rows <= 1000 or not 1 <= args.repeats <= 10:
        parser.error('rows must be 8..1000 and repeats must be 1..10')
    if sys.platform not in ('darwin', 'linux'):
        parser.error('RSS measurement supports macOS and Linux')
    if args.worker:
        print(json.dumps(run_case(args.worker, args.rows), sort_keys=True))
        return
    results = []
    for repeat in range(args.repeats):
        for case in CASES:
            worker = subprocess.run(
                [sys.executable, '-B', '-m', 'spilljoin.demo', '--worker', case,
                 '--rows', str(args.rows)], check=True, capture_output=True, text=True)
            result = json.loads(worker.stdout)
            result['repeat'] = repeat + 1
            results.append(result)
    print(json.dumps({'schema_version': 1, 'python': sys.version,
                      'platform': platform.platform(), 'machine': platform.machine(),
                      'results': results}, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
