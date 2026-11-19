import csv
from collections import Counter
from io import StringIO
from pathlib import Path
import random
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from spilljoin.join import JoinError, ScratchSpace, join_csv


class SpillJoinTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.left = self.root / 'left.csv'
        self.right = self.root / 'right.csv'
        self.output = self.root / 'joined.csv'
        self.scratch = self.root / 'scratch'
        self.scratch.mkdir()

    def write(self, path, rows):
        with path.open('w', encoding='utf-8', newline='') as stream:
            csv.writer(stream).writerows(rows)

    def join(self, **kwargs):
        return join_csv(self.left, self.right, self.output, 'id', 'code',
                        scratch_dir=self.scratch, **kwargs)

    def read(self):
        with self.output.open(encoding='utf-8', newline='') as stream:
            header, *rows = csv.reader(stream)
        self.assertEqual(header, ['left.id', 'left.value', 'right.value', 'right.code'])
        return Counter(map(tuple, rows))

    def assert_clean(self):
        self.assertEqual(list(self.scratch.iterdir()), [])
        self.assertEqual(list(self.root.glob('.spilljoin-*')), [])

    def test_seeded_oracle_memory_forced_and_automatic_spill(self):
        rng = random.Random(73419)
        keys = ['', '001', 'é', 'line\nbreak'] + [str(i) for i in range(97)]
        left = [(rng.choice(keys), f'left,{i}') for i in range(360)]
        right = [(f'right\n{i}', rng.choice(keys)) for i in range(430)]
        for build_side in ('left', 'right'):
            with self.subTest(build_side=build_side):
                # Padding deterministically switches which input file is smaller.
                left_rows = [(key, value + ('X' * 80 if build_side == 'right' else ''))
                             for key, value in left]
                self.write(self.left, [('id', 'value'), *left_rows])
                self.write(self.right, [('value', 'code'), *right])
                self.assertEqual(self.left.stat().st_size < self.right.stat().st_size,
                                 build_side == 'left')
                expected = Counter(a + b for a in left_rows for b in right if a[0] == b[1])
                for options in ({}, {'force_spill': True}, {'build_limit': 32000}):
                    with self.subTest(options=options):
                        partition = ScratchSpace.partition
                        with patch.object(ScratchSpace, 'partition', autospec=True,
                                          side_effect=partition) as partition_calls:
                            self.assertEqual(self.join(**options), sum(expected.values()))
                        self.assertEqual(partition_calls.call_count, 2 if options else 0)
                        self.assertEqual(self.read(), expected)
                        self.assert_clean()
                        self.output.unlink()

    def test_scratch_quota_counts_encoded_rows_from_both_inputs(self):
        left = [('A', 'é,\nvalue')]
        right = [('match', 'A')]
        self.write(self.left, [('id', 'value'), *left])
        self.write(self.right, [('value', 'code'), *right])
        with StringIO(newline='') as buffer:
            csv.writer(buffer).writerows(left + right)
            required = len(buffer.getvalue().encode('utf-8'))
        with self.assertRaisesRegex(JoinError, 'scratch quota exceeded'):
            self.join(force_spill=True, scratch_limit=required - 1)
        self.assertFalse(self.output.exists())
        self.assert_clean()
        self.assertEqual(self.join(force_spill=True, scratch_limit=required), 1)
        self.assertEqual(self.read(), Counter([left[0] + right[0]]))
        self.assert_clean()

    def test_oversized_partition_preserves_output_and_cleans_scratch(self):
        self.write(self.left, [('id', 'value'), *[('hot', str(i)) for i in range(40)]])
        self.write(self.right, [('value', 'code'), *[('larger payload', 'hot') for _ in range(40)]])
        with self.assertRaisesRegex(JoinError, 'partition working set exceeds'):
            self.join(build_limit=1500)
        self.assertFalse(self.output.exists())
        self.assert_clean()

    def test_late_input_failure_cleans_partition_files(self):
        self.write(self.left, [('id', 'value'), ('A', 'valid')])
        self.write(self.right, [('value', 'code'), ('long payload', 'A'), ('bad',)])
        with self.assertRaisesRegex(JoinError, 'wrong number of fields'):
            self.join(force_spill=True)
        self.assertFalse(self.output.exists())
        self.assert_clean()

    def test_scratch_write_failure_cleans_output_and_partitions(self):
        self.write(self.left, [('id', 'value'), ('A', 'valid')])
        self.write(self.right, [('value', 'code'), ('match', 'A')])
        original_open = Path.open

        class BrokenStream:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def write(self, value):
                raise OSError('injected scratch write failure')

        def open_partition(path, mode='r', *args, **kwargs):
            if mode == 'wb' and self.scratch in path.parents:
                # Leave a real file to exercise cleanup after partial creation.
                original_open(path, 'wb').close()
                return BrokenStream()
            return original_open(path, mode, *args, **kwargs)

        with patch.object(Path, 'open', open_partition):
            with self.assertRaisesRegex(OSError, 'injected scratch write failure'):
                self.join(force_spill=True)
        self.assertFalse(self.output.exists())
        self.assert_clean()

    def test_forced_spill_with_header_only_input(self):
        self.write(self.left, [('id', 'value')])
        self.write(self.right, [('value', 'code'), ('unmatched', 'A')])
        self.assertEqual(self.join(force_spill=True), 0)
        self.assertEqual(self.read(), Counter())
        self.assert_clean()

    def test_invalid_limits_and_existing_output(self):
        self.write(self.left, [('id', 'value'), ('A', 'value')])
        self.write(self.right, [('value', 'code'), ('match', 'A')])
        for options in ({'build_limit': 0}, {'scratch_limit': 0}, {'scratch_limit': -1}):
            with self.subTest(options=options):
                with self.assertRaisesRegex(JoinError, 'limit must be positive'):
                    self.join(**options)
                self.assertFalse(self.output.exists())
                self.assert_clean()
        self.output.write_text('preserved')
        with self.assertRaisesRegex(JoinError, 'output already exists'):
            self.join(force_spill=True)
        self.assertEqual(self.output.read_text(), 'preserved')
        self.assert_clean()

    def test_cli_forced_spill_and_scratch_options(self):
        self.write(self.left, [('id', 'value'), ('A', 'value')])
        self.write(self.right, [('value', 'code'), ('match', 'A')])
        command = [sys.executable, '-B', '-m', 'spilljoin', str(self.left), str(self.right),
                   '--left-key', 'id', '--right-key', 'code', '-o', str(self.output),
                   '--force-spill', '--scratch-dir', str(self.scratch), '--scratch-limit-mib', '1']
        subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertEqual(self.read(), Counter([('A', 'value', 'match', 'A')]))
        self.assert_clean()
        self.output.unlink()
        command[-1] = '0'
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn('scratch limit must be positive', result.stderr)
        self.assertFalse(self.output.exists())
        self.assert_clean()
