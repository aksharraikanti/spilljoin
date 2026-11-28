# Demo and benchmark protocol

From a fresh checkout with Python 3.9+ on macOS or Linux, no installation or
third-party dependencies are needed:

```sh
python3 -B -m unittest discover -s tests -v
python3 -B -m spilljoin.demo
python3 -B -m spilljoin.demo --rows 256 --repeats 5 > demo-results.json
```

The demo generates deterministic CSVs in private temporary directories. Every
measurement runs in a fresh Python worker, joins the inputs, samples peak RSS,
then compares the complete output multiset and header to a reference join.
Temporary inputs and output are removed. A mismatch or missing skew fallback
exits unsuccessfully. The offline suite exercises the same entry point.

The memory and forced cases use identical unique-key inputs and an 8 MiB build
budget; each emits N pairs. The skew case uses a single hot key, a 1,500-byte
build budget, bounded repartitioning and chunk-and-rescan; it emits N squared
pairs. All cases use N rows per input. Rows are bounded to 8..1000 and repeats
to 1..10 so this correctness demonstration has a predictable maximum workload.

## Reading the report

The JSON includes Python and platform metadata, case, repeat, rows, budget,
elapsed join seconds, peak RSS bytes and operator counters. RSS is the worker's
process lifetime high-water mark sampled immediately after the join, before
oracle construction and output verification. It includes interpreter startup,
imports and fixture generation, and is not incremental join memory. macOS
reports bytes; Linux reports KiB, which the harness converts to bytes.

Join timing excludes fixture generation, worker startup and verification.
It includes parsing, output, partitioning and atomic publication. RSS and time
vary with Python, allocator, OS, filesystem and cache state. Keep raw samples;
compare median and range across fresh workers. Forced and memory runs are
comparable for the same N. Skew runs do more output work and should not be
interpreted as an equivalent throughput comparison.

scratch_written_bytes is cumulative encoded scratch output, not physical disk
I/O. probe_passes indicates repeated scans but is not a read-byte count. Do not
claim physical I/O measurements from these fields.

## Release evidence still required

No measured performance numbers are published in this change. The automated
demo tests verify correctness and report shape; they do not establish performance.

Before release, collect and retain raw results on a documented machine and
filesystem, record the source commit, CPU, RAM, Python version, command, budgets,
input sizes, output counts and cache conditions, and explain the measurements.
Add logical scratch-read byte accounting or separately measured scratch reads
with precise semantics. Include larger streaming workloads beyond this small
demo and show the effect of the build budget on RSS, scratch traffic and time.
Publish results and limitations without a machine-independent speed claim.
