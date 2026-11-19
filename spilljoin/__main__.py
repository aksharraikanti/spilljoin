"""Run the SpillJoin command-line tool."""

import argparse
import csv

from . import __version__
from .join import JoinError, join_csv


def main():
    parser = argparse.ArgumentParser(prog='spilljoin', description='Inner equijoin for two UTF-8 CSV files.')
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('left')
    parser.add_argument('right')
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('--left-key', required=True)
    parser.add_argument('--right-key', required=True)
    parser.add_argument('--build-limit-mib', type=int, default=64)
    parser.add_argument('--scratch-limit-mib', type=int, default=1024)
    parser.add_argument('--scratch-dir', help='parent directory for temporary partitions')
    parser.add_argument('--force-spill', action='store_true', help='partition even when the build fits in memory')
    args = parser.parse_args()
    try:
        join_csv(args.left, args.right, args.output, args.left_key, args.right_key,
                 build_limit=args.build_limit_mib * 1024 * 1024,
                 scratch_limit=args.scratch_limit_mib * 1024 * 1024,
                 scratch_dir=args.scratch_dir, force_spill=args.force_spill)
    except (JoinError, OSError, UnicodeError, csv.Error) as error:
        parser.exit(1, f'spilljoin: {error}\n')


if __name__ == '__main__':
    main()
