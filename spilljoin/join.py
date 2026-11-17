"""Streaming CSV probe and in-memory build for an inner equijoin."""

import csv
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


class JoinError(ValueError):
    """An invalid input or an unsupported working set."""


class CsvInput:
    def __init__(self, stream, side, key):
        self.reader = csv.reader(stream, strict=True)
        self.side = side
        self.header = next(self.reader, [])
        if not self.header or any(not column for column in self.header):
            raise JoinError(f'{side} input needs a nonempty header')
        if len(set(self.header)) != len(self.header):
            raise JoinError(f'{side} input has duplicate column names')
        if key not in self.header:
            raise JoinError(f'{side} input has no column named {key!r}')
        self.key = self.header.index(key)

    def __iter__(self):
        for row in self.reader:
            if len(row) != len(self.header):
                raise JoinError(f'{self.side} input has the wrong number of fields at line {self.reader.line_num}')
            yield row


def join_csv(left_path, right_path, output_path, left_key, right_key, *, build_limit=64 * 1024 * 1024):
    """Write a new CSV and return the number of joined rows."""
    if build_limit < 1:
        raise JoinError('build limit must be positive')
    left_path, right_path, output_path = map(Path, (left_path, right_path, output_path))
    if os.path.lexists(output_path):
        raise JoinError('output already exists')
    staging = None
    try:
        with left_path.open(encoding='utf-8', newline='') as left_stream, \
                right_path.open(encoding='utf-8', newline='') as right_stream:
            left = CsvInput(left_stream, 'left', left_key)
            right = CsvInput(right_stream, 'right', right_key)
            build, probe = (left, right) if os.fstat(left_stream.fileno()).st_size <= os.fstat(
                right_stream.fileno()).st_size else (right, left)
            index = {}
            retained = 0
            for row in build:
                retained += 256 + sum(80 + 4 * len(field) for field in row)
                if retained > build_limit:
                    raise JoinError('estimated build working set exceeds the limit; disk spilling is pending')
                index.setdefault(row[build.key], []).append(row)
            with NamedTemporaryFile(mode='w', encoding='utf-8', newline='', dir=output_path.parent,
                                    prefix='.spilljoin-', delete=False) as stream:
                staging = stream.name
                writer = csv.writer(stream)
                writer.writerow([f'left.{name}' for name in left.header] +
                                [f'right.{name}' for name in right.header])
                count = 0
                for row in probe:
                    for match in index.get(row[probe.key], ()):
                        writer.writerow(match + row if build is left else row + match)
                        count += 1
            # A same-directory hard link publishes atomically and refuses a racing overwrite.
            os.link(staging, output_path)
            return count
    finally:
        if staging is not None:
            Path(staging).unlink()
