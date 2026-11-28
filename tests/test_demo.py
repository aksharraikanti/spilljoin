import json
import subprocess
import sys
import unittest


class DemoTests(unittest.TestCase):
    def test_demo_verifies_all_paths_with_fresh_workers(self):
        result = subprocess.run(
            [sys.executable, '-B', '-m', 'spilljoin.demo', '--rows', '8', '--repeats', '2'],
            check=True, capture_output=True, text=True)
        report = json.loads(result.stdout)
        self.assertEqual(report['schema_version'], 1)
        self.assertTrue(report['python'])
        self.assertTrue(report['platform'])
        self.assertEqual(len(report['results']), 6)
        for repeat in (1, 2):
            cases = {row['case']: row for row in report['results'] if row['repeat'] == repeat}
            self.assertEqual(set(cases), {'memory', 'forced', 'skew'})
            for row in cases.values():
                self.assertTrue(row['verified'])
                self.assertGreater(row['peak_rss_bytes'], 0)
                self.assertLessEqual(row['metrics']['retained_peak_bytes'], row['build_limit_bytes'])
            self.assertFalse(cases['memory']['metrics']['spilled'])
            self.assertTrue(cases['forced']['metrics']['spilled'])
            self.assertEqual(cases['forced']['metrics']['output_rows'], 8)
            self.assertEqual(cases['skew']['metrics']['output_rows'], 64)
            self.assertEqual(cases['skew']['metrics']['fallback_partitions'], 1)

    def test_demo_rejects_unbounded_workload_arguments(self):
        for arguments in (['--rows', '0'], ['--rows', '1001'], ['--repeats', '11']):
            result = subprocess.run(
                [sys.executable, '-B', '-m', 'spilljoin.demo', *arguments],
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn('rows must be 8..1000', result.stderr)
