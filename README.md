# SpillJoin

SpillJoin joins two unsorted UTF-8 CSV files on named columns. It builds a hash table from the smaller input file and streams the other input. When the estimated build state exceeds its budget, it partitions both inputs on disk and joins each partition in turn.

From a checkout with Python 3.9 or later, run the join and offline tests with these commands.

```sh
python3 -m spilljoin events.csv accounts.csv --left-key account_id --right-key id -o joined.csv
python3 -B -m unittest discover -s tests -v
```

Both inputs need headers with unique names. Keys compare as strings, including empty keys and leading zeros. Duplicate keys produce every matching pair. The output includes both keys and qualifies columns with `left.` or `right.`. Row order is unspecified.

## Build budget and scratch space

`--build-limit-mib` defaults to 64. The retained-state estimate charges 256 bytes per row plus 80 bytes and four bytes per character for each field. This includes a conservative allowance for rows and index overhead. Python's runtime, transient CSV buffers, partition streams and the current row also consume memory. A single input row must fit in memory, and the standard-library CSV field-size limit applies.

On budget overflow, the buffered rows and remaining build input are distributed among 64 partitions using a stable hash of the join key. The probe input uses the same hash. An oversized partition is repartitioned with a depth-specific hash, at most twice. After that, SpillJoin loads build chunks within the budget and rescans the matching probe partition for each chunk. This preserves every duplicate pair even for a single hot key; it trades repeated reads for bounded retained state. A single build row whose estimate exceeds the budget raises an error. The budget is an estimate, not an operating-system RSS limit.

`--scratch-limit-mib` defaults to 1024 and caps the total encoded CSV bytes written across both inputs' partitions, including every repartitioning pass. Deleting a partition does not refund the quota. File-system metadata, allocation rounding and the output file are outside that quota. `--scratch-dir` chooses an existing parent directory for a private temporary directory. Otherwise the operating system's temporary directory is used. Scratch files are cleaned on success and handled failures. An abrupt process termination may leave temporary files.

Use `--force-spill` to exercise partitioning even when the build fits in memory. This example uses the same inputs as the quickstart and creates a separate output.

```sh
python3 -m spilljoin events.csv accounts.csv --left-key account_id --right-key id \
  --force-spill --build-limit-mib 8 --scratch-limit-mib 256 --scratch-dir /tmp -o spilled.csv
```

A successful run creates a new output file. Existing paths are preserved, and parse, quota and partition-budget failures discard the temporary output. Publication uses a same-directory hard link that refuses an overwrite, including when another process creates the destination during the join. The output file system must support hard links.

See [ROADMAP.md](ROADMAP.md) for milestone progress and release criteria.

## Metrics and reproducible demo

Add --metrics to a join command to emit one JSON success record on stderr.
The CSV output and Python API row-count return value are unchanged. Counters
include chosen build side, spill use, output rows, scratch bytes written,
repartition and fallback counts, probe scans, peak estimated retained state,
and elapsed join seconds. Failed commands emit an error without success metrics.

```sh
python3 -B -m spilljoin.demo
python3 -B -m spilljoin.demo --rows 256 --repeats 5 > demo-results.json
```

The demo generates inputs, verifies complete output multisets and exercises
memory, forced spill and hot-key fallback in fresh workers. It needs no packages
or input downloads. See [BENCHMARKS.md](BENCHMARKS.md) for measurement semantics
and remaining release evidence, and [ARCHITECTURE.md](ARCHITECTURE.md) for the
algorithm, metrics contract, complexity and operational limits.
