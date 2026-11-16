# SpillJoin

Join unsorted CSV files with a bounded build working set and disk-backed partitions.

Build a Python 3.9+ standard-library CLI for an inner equijoin on named string keys in two unsorted UTF-8 CSV inputs. Preserve duplicate-key multiplicity, stream output, and implement disk-backed hash partitioning and a bounded fallback for skew. Emit operator metrics and document reproducible memory, I/O and throughput measurements. Support macOS and Linux. Leave SQL, outer joins and parallel execution outside v1.0.

Run `python3 -m spilljoin --version` from a checkout. See [ROADMAP.md](ROADMAP.md) for implementation milestones.

Run the suite with `python3 -B -m unittest discover -s tests -v`.
