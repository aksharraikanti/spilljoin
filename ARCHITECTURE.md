# Architecture and limits

The smaller file by encoded byte size is the build input. This is a heuristic:
short records can have a larger Python footprint than fewer long records. Headers
are validated first; output always lists the left columns before the right ones.

The in-memory path retains a dictionary of key-to-row lists, preserving duplicate
multiplicity. It streams the probe and the output. Each retained row is charged
256 bytes plus 80 bytes and four bytes per character per field. The estimate
covers retained rows and index overhead, not the entire process. CSV parser
buffers, the current row, Python itself and output buffers are outside it.

When the next row would exceed the estimate, retained and unread build rows go
to 64 stable BLAKE2b buckets. The probe uses the same buckets. Oversized buckets
use two additional depth-specific hashes. A still-oversized bucket is read as
bounded chunks, rescanning the probe bucket for each chunk. Recursion terminates
even if every key is identical. A build row larger than the budget is rejected.

For build rows B, probe rows P and output pairs O, the in-memory path takes
expected O(B + P + O) work. Partitioning adds encoded writes and reads. In the hot
key fallback with C chunks, probe work is O(C * P + O); the many-to-many output
itself can be quadratic. Partition files are removed as their joins finish, but
the scratch quota counts cumulative encoded writes across all passes. It never
refunds deleted bytes. File metadata, allocated blocks, reads and output bytes
are outside that quota. There is no claim of bounded wall time or output size.

Temporary scratch directories are private. A staging file in the destination
directory receives output; a hard link publishes it only after successful
parsing and joining. This refuses racing overwrites. Handled failures remove
temporary state. Hard links must be supported. This is atomic publication,
not a crash-durability protocol: there is no fsync, and abrupt termination can
leave staging or scratch files. Input files must remain unchanged during a join.

## Metrics contract

The Python API still returns the row count. Passing a dictionary as metrics=
updates it after successful publication. Failures leave the dictionary unchanged.
The CLI's --metrics option prints one JSON object on stderr after success.

- output_rows counts data rows, excluding the header.
- build_side identifies the smaller-file choice (ties choose left).
- spilled includes both automatic and forced partitioning.
- scratch_written_bytes counts encoded CSV bytes across all partition writes,
  including repartitions. It excludes headers, file-system metadata and output.
- repartition_passes counts oversized partition pairs split with a new hash.
- fallback_partitions counts partition pairs handled by chunk-and-rescan.
- probe_passes counts complete probe scans, including empty partition scans and
  each fallback chunk. It is an algorithm counter, not a physical I/O measure.
- retained_peak_bytes is the highest charged build state actually retained.
  The overflowing row is not charged to this peak until it is retained in a
  partition or chunk; transient memory is excluded.
- elapsed_seconds uses a monotonic clock from API entry through publication.
  Final staging unlink and CLI JSON serialization are outside the interval.

These counters are per call. They expose algorithm choices and quota usage;
they do not measure actual disk traffic, RSS or a hard memory limit. Logical
scratch-read byte accounting and representative measured benchmark results
remain release work.
