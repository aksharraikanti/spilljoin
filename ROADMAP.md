# SpillJoin roadmap

## Milestones

- [x] Implement a streaming in-memory build and probe CLI with duplicate-key joins, correct CSV behavior, atomic output and deterministic error tests.
- [x] Add disk-backed hash partitions with working-set accounting and scratch quotas. Compare forced-spill output against the in-memory path and seeded reference joins.
- [ ] Bound repartitioning and implement chunk-and-rescan for hot keys. Verify skew, scratch exhaustion, I/O failure, cleanup and exact many-to-many results.
- [ ] Publish a reproducible demo, measured benchmarks, architecture and limitations documentation, and a validated v1.0.0 release.

## Release criteria

- Complete every milestone with deterministic offline tests and a green required GitHub Actions check.
- Verify complete output multisets against a seeded oracle, including skew and duplicates, plus scratch cleanup on tested failures.
- Run the dependency-free demo from a fresh checkout and show forced spill and the skew fallback.
- Publish measured wall time, peak RSS, scratch I/O and row counts. Resolve all release-blocking defects.
- Validate the merged tree and publish v1.0.0 through the controller.

## Milestone 2 acceptance coverage

- Seeded full-output multiset comparisons cover the in-memory, forced-spill and automatic-spill paths, both build orientations, duplicate keys, empty keys, Unicode and multiline fields.
- Quota boundary tests count encoded UTF-8 CSV bytes from both inputs and check cleanup after exhaustion.
- Failure cases cover a hot partition exceeding the retained-state budget, malformed input after partition creation, injected scratch-write failure, invalid limits and existing output preservation.
- CLI tests exercise forced spill, scratch directory selection and quota validation. Header-only inputs remain supported.
- Each partition currently must fit the build budget. Bounded repartitioning and chunk-and-rescan remain milestone 3 work. Operator metrics, measured benchmarks and the standalone demo remain milestone 4 work.
