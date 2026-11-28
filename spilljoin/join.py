"""Streaming CSV inner equijoin with budget-triggered hash partitioning."""

from contextlib import ExitStack
import csv
from dataclasses import asdict, dataclass
import hashlib
from io import StringIO
from itertools import chain
import os
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from time import perf_counter


PARTITION_COUNT = 64
MAX_REPARTITION_DEPTH = 2


@dataclass
class JoinMetrics:
    output_rows: int = 0
    build_side: str = ''
    spilled: bool = False
    scratch_written_bytes: int = 0
    repartition_passes: int = 0
    fallback_partitions: int = 0
    probe_passes: int = 0
    retained_peak_bytes: int = 0
    elapsed_seconds: float = 0.0


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
    def __init__(self, root, limit, metrics=None):
        self.root = Path(root)
        self.limit = limit
        self.written = 0
        self.sequence = 0
        self.metrics = metrics if metrics is not None else JoinMetrics()

    def partition(self, rows, key, side, depth=0):
        self.sequence += 1
        paths = [self.root / f'{self.sequence}-{side}-{number}.csv' for number in range(PARTITION_COUNT)]
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
                    digest = hashlib.blake2b(bytes([depth]) + row[key].encode('utf-8'), digest_size=8).digest()
                    bucket = int.from_bytes(digest, 'big') % PARTITION_COUNT
                    streams[bucket].write(encoded)
                    self.written += len(encoded)
                    self.metrics.scratch_written_bytes += len(encoded)
        return paths


def _partition_file(scratch, path, key, side, depth):
    with path.open(encoding='utf-8', newline='') as stream:
        return scratch.partition(csv.reader(stream, strict=True), key, side, depth)


def _probe_file(writer, path, key, index, build_is_left):
    with path.open(encoding='utf-8', newline='') as stream:
        return _probe(writer, csv.reader(stream, strict=True), key, index, build_is_left)


def _chunk_join(writer, build_path, probe_path, build_key, probe_key,
                build_is_left, build_limit, metrics):
    # One bounded build chunk at a time; rescan the probe for every chunk.
    metrics.fallback_partitions += 1
    count = 0
    index = {}
    retained = 0
    with build_path.open(encoding='utf-8', newline='') as stream:
        for row in csv.reader(stream, strict=True):
            size = _row_size(row)
            if size > build_limit:
                raise JoinError('single build row exceeds the build limit')
            if retained + size > build_limit:
                metrics.probe_passes += 1
                count += _probe_file(writer, probe_path, probe_key, index, build_is_left)
                index.clear()
                retained = 0
            index.setdefault(row[build_key], []).append(row)
            retained += size
            metrics.retained_peak_bytes = max(metrics.retained_peak_bytes, retained)
    if index:
        metrics.probe_passes += 1
        count += _probe_file(writer, probe_path, probe_key, index, build_is_left)
    return count


def _join_partition(writer, scratch, build_path, probe_path, build_key, probe_key,
                    build_is_left, build_limit, depth):
    metrics = scratch.metrics
    index = {}
    retained = 0
    oversized = False
    with build_path.open(encoding='utf-8', newline='') as stream:
        for row in csv.reader(stream, strict=True):
            size = _row_size(row)
            if size > build_limit:
                raise JoinError('single build row exceeds the build limit')
            if retained + size > build_limit:
                oversized = True
                break
            index.setdefault(row[build_key], []).append(row)
            retained += size
            metrics.retained_peak_bytes = max(metrics.retained_peak_bytes, retained)
    if not oversized:
        metrics.probe_passes += 1
        return _probe_file(writer, probe_path, probe_key, index, build_is_left)
    index.clear()
    if depth == MAX_REPARTITION_DEPTH:
        return _chunk_join(writer, build_path, probe_path, build_key, probe_key,
                           build_is_left, build_limit, metrics)
    metrics.repartition_passes += 1
    next_depth = depth + 1
    build_paths = _partition_file(scratch, build_path, build_key, 'build', next_depth)
    probe_paths = _partition_file(scratch, probe_path, probe_key, 'probe', next_depth)
    # Quota is cumulative bytes written, so deletion never refunds it.
    build_path.unlink()
    probe_path.unlink()
    count = 0
    for child_build, child_probe in zip(build_paths, probe_paths):
        count += _join_partition(writer, scratch, child_build, child_probe,
                                 build_key, probe_key, build_is_left, build_limit, next_depth)
        child_build.unlink(missing_ok=True)
        child_probe.unlink(missing_ok=True)
    return count


def _spill_join(writer, build_rows, build_key, probe_rows, probe_key, build_is_left,
                build_limit, scratch_limit, scratch_dir, metrics):
    metrics.spilled = True
    with TemporaryDirectory(prefix='.spilljoin-', dir=scratch_dir) as directory:
        scratch = ScratchSpace(directory, scratch_limit, metrics)
        build_paths = scratch.partition(build_rows, build_key, 'build')
        probe_paths = scratch.partition(probe_rows, probe_key, 'probe')
        count = 0
        for build_path, probe_path in zip(build_paths, probe_paths):
            count += _join_partition(writer, scratch, build_path, probe_path, build_key,
                                     probe_key, build_is_left, build_limit, 0)
            build_path.unlink(missing_ok=True)
            probe_path.unlink(missing_ok=True)
        return count


def join_csv(left_path, right_path, output_path, left_key, right_key, *,
             build_limit=64 * 1024 * 1024, scratch_limit=1024 * 1024 * 1024,
             scratch_dir=None, force_spill=False, metrics=None):
    """Return joined row count; update an optional metrics dict only on success."""
    started = perf_counter()
    stats = JoinMetrics()
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
            stats.build_side = 'left' if build is left else 'right'
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
                                        build_limit, scratch_limit, scratch_dir, stats)
                else:
                    for row in build_rows:
                        retained += _row_size(row)
                        if retained > build_limit:
                            pending_rows = chain(_drain_index(index), (row,), build_rows)
                            count = _spill_join(writer, pending_rows, build.key, probe, probe.key,
                                                build is left, build_limit, scratch_limit, scratch_dir, stats)
                            break
                        index.setdefault(row[build.key], []).append(row)
                        stats.retained_peak_bytes = max(stats.retained_peak_bytes, retained)
                    else:
                        stats.probe_passes += 1
                        count = _probe(writer, probe, probe.key, index, build is left)
            # A same-directory hard link publishes atomically and refuses a racing overwrite.
            os.link(staging, output_path)
            stats.output_rows = count
            stats.elapsed_seconds = perf_counter() - started
            if metrics is not None:
                metrics.update(asdict(stats))
            return count
    finally:
        if staging is not None:
            Path(staging).unlink()
