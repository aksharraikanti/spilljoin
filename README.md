# SpillJoin

SpillJoin joins two unsorted UTF-8 CSV files on named columns. It builds a hash table from one input and streams the other. Disk-backed partitions are the next milestone.

From a checkout with Python 3.9 or later:

```sh
python3 -m spilljoin events.csv accounts.csv --left-key account_id --right-key id -o joined.csv
python3 -B -m unittest discover -s tests -v
```

Both inputs need headers with unique names. Keys compare as strings, including empty keys and leading zeros. Duplicate keys produce every matching pair. The output includes both keys and qualifies columns with `left.` or `right.`. Row order is unspecified.

The command picks the build side by input file size and stops when the estimated retained build state exceeds `--build-limit-mib`, which defaults to 64. The estimate covers retained rows and index overhead. Python's runtime and transient CSV buffers also consume memory. A successful run creates a new output file. Existing paths are preserved, and parse failures discard the temporary output.

See [ROADMAP.md](ROADMAP.md) for the planned spill, skew and benchmark work.
