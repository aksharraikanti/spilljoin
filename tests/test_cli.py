import subprocess
import sys
import unittest


class CommandLineTests(unittest.TestCase):
    def test_version(self):
        result = subprocess.run(
            [sys.executable, "-B", "-m", "spilljoin", "--version"],
            check=True, capture_output=True, text=True,
        )
        self.assertRegex(result.stdout, r"^\d+\.\d+\.\d+\n$")
