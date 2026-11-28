import csv
from io import StringIO
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

from spilljoin.join import JoinError, join_csv


class MetricsTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.left = self.root / 'left.csv'
        self.right = self.root / 'right.csv'
        self.output = self.root / 'joined.csv'

    def inputs(self, rows=1, hot=False, reverse=False):
        left = [(('hot' if hot else str(i)), 'é,\n' + str(i)) for i in range(rows)]
        right = [(('hot' if hot else str(i)), 'longer payload ' + str(i)) for i in range(rows)]
        if reverse:
            left = [(key, value + 'X' * 80) for key, value in left]
        for path, header, values in ((self.left, ('id', 'value'), left),
                                      (self.right, ('code', 'payload'), right)):
            with path.open('w', encoding='utf-8', newline='') as stream:
                csv.writer(stream).writerows([header, *values])
        return left, right

    def join(self, metrics, **options):
        return join_csv(self.left, self.right, self.output, 'id', 'code',
                        metrics=metrics, **options)

    def test_memory_metrics_and_both_build_orientations(self):
        for reverse in (False, True):
            self.inputs(rows=4, reverse=reverse)
            metrics = {}
            self.assertEqual(self.join(metrics), 4)
            self.assertEqual(metrics['build_side'], 'right' if reverse else 'left')
            self.assertEqual(metrics['output_rows'], 4)
            self.assertFalse(metrics['spilled'])
            self.assertEqual(metrics['scratch_written_bytes'], 0)
            self.assertEqual(metrics['probe_passes'], 1)
            self.assertGreater(metrics['retained_peak_bytes'], 0)
            self.assertGreaterEqual(metrics['elapsed_seconds'], 0)
            self.output.unlink()

    def test_scratch_bytes_include_csv_encoding(self):
        left, right = self.inputs()
        with StringIO(newline='') as stream:
            csv.writer(stream).writerows(left + right)
            expected = len(stream.getvalue().encode('utf-8'))
        metrics = {}
        self.assertEqual(self.join(metrics, force_spill=True), 1)
        self.assertTrue(metrics['spilled'])
        self.assertEqual(metrics['scratch_written_bytes'], expected)
        self.assertEqual(metrics['repartition_passes'], 0)
        self.assertEqual(metrics['fallback_partitions'], 0)
        self.assertGreater(metrics['retained_peak_bytes'], 0)

    def test_hot_key_metrics_for_forced_and_automatic_spill(self):
        for force_spill in (False, True):
            left, right = self.inputs(rows=12, hot=True)
            with StringIO(newline='') as stream:
                csv.writer(stream).writerows(left + right)
                initial_bytes = len(stream.getvalue().encode('utf-8'))
            metrics = {}
            self.assertEqual(self.join(metrics, build_limit=1500, force_spill=force_spill), 144)
            self.assertEqual(metrics['repartition_passes'], 2)
            self.assertEqual(metrics['fallback_partitions'], 1)
            self.assertEqual(metrics['scratch_written_bytes'], 3 * initial_bytes)
            self.assertLessEqual(metrics['retained_peak_bytes'], 1500)
            self.assertGreater(metrics['probe_passes'], 64)
            self.output.unlink()

    def test_failure_does_not_publish_success_metrics(self):
        self.inputs()
        metrics = {'previous': 'preserved'}
        with self.assertRaises(JoinError):
            self.join(metrics, force_spill=True, scratch_limit=1)
        self.assertEqual(metrics, {'previous': 'preserved'})
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.glob('.spilljoin-*')), [])

    def test_cli_metrics_are_json_on_stderr_only_after_success(self):
        self.inputs()
        command = [sys.executable, '-B', '-m', 'spilljoin', str(self.left), str(self.right),
                   '--left-key', 'id', '--right-key', 'code', '-o', str(self.output), '--metrics']
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertEqual(result.stdout, '')
        self.assertEqual(json.loads(result.stderr)['output_rows'], 1)
        failed = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(failed.returncode, 1)
        self.assertIn('output already exists', failed.stderr)
        self.assertNotIn('output_rows', failed.stderr)
