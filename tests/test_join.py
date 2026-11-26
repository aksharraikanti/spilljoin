import csv
from collections import Counter
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from spilljoin.join import JoinError, join_csv


class CsvJoinTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.left = self.root / 'left.csv'
        self.right = self.root / 'right.csv'
        self.output = self.root / 'out.csv'

    def write(self, path, rows):
        with path.open('w', encoding='utf-8', newline='') as stream:
            csv.writer(stream).writerows(rows)

    def read(self):
        with self.output.open(encoding='utf-8', newline='') as stream:
            return list(csv.reader(stream))

    def join(self, **kwargs):
        return join_csv(self.left, self.right, self.output, 'id', 'code', **kwargs)

    def test_duplicate_string_keys_and_csv_round_trip(self):
        left = [('001', 'a\nline'), ('001', 'b,c'), ('', 'é'), ('1', 'unmatched')]
        right = [('001', 'x'), ('001', 'y'), ('', 'empty match')]
        self.write(self.left, [('id', 'value'), *left])
        self.write(self.right, [('code', 'value'), *right])
        self.assertEqual(self.join(), 5)
        header, *rows = self.read()
        self.assertEqual(header, ['left.id', 'left.value', 'right.code', 'right.value'])
        self.assertEqual(Counter(map(tuple, rows)), Counter(a + b for a in left for b in right if a[0] == b[0]))

    def test_header_only_input(self):
        self.write(self.left, [('id',)])
        self.write(self.right, [('code',), ('A',)])
        self.assertEqual(self.join(), 0)
        self.assertEqual(self.read(), [['left.id', 'right.code']])

    def test_late_bad_row_removes_staging(self):
        self.write(self.left, [('id',), ('A',)])
        self.write(self.right, [('code', 'value'), ('A', 'long matching payload'), ('A',)])
        with self.assertRaisesRegex(JoinError, 'wrong number of fields'):
            self.join()
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.spilljoin-*')), [])

    def test_invalid_header_and_no_overwrite(self):
        self.write(self.left, [('id', 'id')])
        self.write(self.right, [('code',)])
        with self.assertRaisesRegex(JoinError, 'duplicate column'):
            self.join()
        self.output.write_text('keep me')
        with self.assertRaisesRegex(JoinError, 'already exists'):
            self.join()
        self.assertEqual(self.output.read_text(), 'keep me')

    def test_small_build_budget_stops_before_publication(self):
        self.write(self.left, [('id',), ('A',)])
        self.write(self.right, [('code',), ('A',)])
        with self.assertRaisesRegex(JoinError, 'single build row exceeds'):
            self.join(build_limit=1)
        self.assertFalse(self.output.exists())

    def test_cli_joins_two_files(self):
        self.write(self.left, [('id',), ('A',)])
        self.write(self.right, [('code',), ('A',)])
        subprocess.run([sys.executable, '-B', '-m', 'spilljoin', str(self.left), str(self.right),
                        '--left-key', 'id', '--right-key', 'code', '-o', str(self.output)], check=True)
        self.assertEqual(self.read()[-1], ['A', 'A'])
