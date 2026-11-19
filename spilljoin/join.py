"""Streaming CSV inner equijoin with budget-triggered hash partitioning."""

from contextlib import ExitStack
import csv
import hashlib
from io import StringIO
from itertools import chain
import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory


PARTITION_COUNT = 64


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


def _row_size(row):
    # Conservative retained-row and hash-index estimate, excluding transient buffers.
    return 256 + sum(80 + 4 * len(field) for field in row)


def _drain_index(index):
    while index:
        _, rows = index.popitem()
        yield from rows


def _probe(writer, rows, key, index, build_is_left):
    count = 0
    for row in rows:
        for match in index.get(row[key], ()):
            writer.writerow(match + row if build_is_left else row + match)
            count += 1
    return count


class ScratchSpace:
    def __init__(self, root, limit):
        self.root = Path(root)
        self.limit = limit
        self.written = 0

    def partition(self, rows, key, side):
        paths = [self.root / f'{side}-{number}.csv' for number in range(PARTITION_COUNT)]
        with ExitStack() as stack:
            streams = [stack.enter_context(path.open('wb')) for path in paths]
            with StringIO(newline='') as buffer:
                writer = csv.writer(buffer)
                for row in rows:
                    buffer.seek(0)
                    buffer.truncate()
                    writer.writerow(row)
                    encoded = buffer.getvalue().encode('utf-8')
                    if len(encoded) > self.limit - self.written:
                        raise JoinError('scratch quota exceeded')
                    digest = hashlib.blake2b(row[key].encode('utf-8'), digest_size=8).digest()
                    bucket = int.from_bytes(digest, 'big') % PARTITION_COUNT
                    streams[bucket].write(encoded)
                    self.written += len(encoded)
        return paths


def _spill_join(writer, build_rows, build_key, probe_rows, probe_key, build_is_left,
                build_limit, scratch_limit, scratch_dir):
    with TemporaryDirectory(prefix='.spilljoin-', dir=scratch_dir) as directory:
        scratch = ScratchSpace(directory, scratch_limit)
        build_paths = scratch.partition(build_rows, build_key, 'build')
        probe_paths = scratch.partition(probe_rows, probe_key, 'probe')
        count = 0
        for build_path, probe_path in zip(build_paths, probe_paths):
            index = {}
            retained = 0
            with build_path.open(encoding='utf-8', newline='') as stream:
                for row in csv.reader(stream, strict=True):
                    retained += _row_size(row)
                    if retained > build_limit:
                        raise JoinError('partition working set exceeds the build limit; skew fallback is pending')
                    index.setdefault(row[build_key], []).append(row)
            with probe_path.open(encoding='utf-8', newline='') as stream:
                count += _probe(writer, csv.reader(stream, strict=True), probe_key, index, build_is_left)
            # Release the last partition before allocating the next index.
            index.clear()
        return count


def join_csv(left_path, right_path, output_path, left_key, right_key, *,
             build_limit=64 * 1024 * 1024, scratch_limit=1024 * 1024 * 1024,
             scratch_dir=None, force_spill=False):
    """Write a new CSV and return the joined row count. Limits are in bytes."""
    if build_limit < 1:
        raise JoinError('build limit must be positive')
    if scratch_limit < 1:
        raise JoinError('scratch limit must be positive')
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
            with NamedTemporaryFile(mode='w', encoding='utf-8', newline='', dir=output_path.parent,
                                    prefix='.spilljoin-', delete=False) as stream:
                staging = stream.name
                writer = csv.writer(stream)
                writer.writerow([f'left.{name}' for name in left.header] +
                                [f'right.{name}' for name in right.header])
                build_rows = iter(build)
                index = {}
                retained = 0
                if force_spill:
                    count = _spill_join(writer, build_rows, build.key, probe, probe.key, build is left,
                                        build_limit, scratch_limit, scratch_dir)
                else:
                    for row in build_rows:
                        retained += _row_size(row)
                        if retained > build_limit:
                            pending_rows = chain(_drain_index(index), (row,), build_rows)
                            count = _spill_join(writer, pending_rows, build.key, probe, probe.key,
                                                build is left, build_limit, scratch_limit, scratch_dir)
                            break
                        index.setdefault(row[build.key], []).append(row)
                    else:
                        count = _probe(writer, probe, probe.key, index, build is left)
            # A same-directory hard link publishes atomically and refuses a racing overwrite.
            os.link(staging, output_path)
            return count
    finally:
        if staging is not None:
            Path(staging).unlink()
