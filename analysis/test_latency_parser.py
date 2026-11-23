"""CLI regression cases for the optimized bundled histogram parser."""

import datetime
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'webapp'))
from server import parse_tool_output

TOOL = Path(__file__).resolve().parents[1] / 'asloglatency' / 'log_latency.py'


class LatencyParserTests(unittest.TestCase):
    def run_case(self, histograms=('{test}-read',), histogram='{test}-read', unit='msec',
                 milliseconds=False, reset=False, switch=False, wide=False,
                 incomplete=False, extra=(), expected_exit=0):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'aerospike.log'
            with path.open('w') as out:
                for i in range(8):
                    stamp = (datetime.datetime(2026, 11, 23, 23, 59, 20)
                             + datetime.timedelta(seconds=i * 10)).strftime('%b %d %Y %H:%M:%S')
                    if milliseconds:
                        stamp += '.123'
                    count = i + 1 if not reset or i < 4 else i - 3
                    dump_unit = 'usec' if switch and i >= 4 else unit
                    for hist in histograms:
                        prefix = f'{stamp} GMT: INFO (info): (hist.c:1)'
                        out.write(f'{prefix} histogram dump: {hist} ({count * 1000} total) {dump_unit}\n')
                        out.write(f'{prefix} (00: {count * 900:010d}) (01: {count * 80:010d})\n')
                        if not incomplete or i != 7:
                            bucket = '25' if wide else '04'
                            out.write(f'{prefix} ({bucket}: {count * 20:010d})\n')
                    out.write(f'{stamp} GMT: INFO (ticker): (ticker.c:1) unrelated message\n')
            result = subprocess.run([sys.executable, str(TOOL), '-l', str(path), '-h', histogram,
                                     '-f', 'head', '-t', '10', '-n', '6', '-e', '1', *extra],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, expected_exit, result.stderr)
            return parse_tool_output(result.stdout), result.stdout

    def test_timestamp_formats_and_latency_units(self):
        for milliseconds, unit in [(False, 'msec'), (True, 'msec'), (True, 'usec')]:
            with self.subTest(milliseconds=milliseconds, unit=unit):
                parsed, _ = self.run_case(milliseconds=milliseconds, unit=unit)
                self.assertEqual(parsed['unit'], 'us' if unit == 'usec' else 'ms')
                self.assertEqual(parsed['thresholds'], [1, 2, 4, 8, 16, 32])
                self.assertEqual(parsed['ops'], [100.] * 7)
                self.assertEqual(parsed['overs'], [[v] * 7 for v in [10., 2., 2., 2., 0., 0.]])
                self.assertEqual([b - a for a, b in zip(parsed['times'], parsed['times'][1:])], [10] * 6)

    def test_namespaces_and_filters(self):
        for extra, rate in [((), 200.), (('-N', 'a'), 100.)]:
            parsed, _ = self.run_case(histograms=('{a}-read', '{b}-read'), histogram='read', extra=extra)
            self.assertEqual(parsed['ops'], [rate] * 7)

    def test_wide_bucket_histograms(self):
        for hist, unit in [('{test}-device-read-size', 'bytes'), ('{test}-query-rec-count', 'records')]:
            parsed, _ = self.run_case(histograms=(hist,), histogram=hist, wide=True, unit='')
            self.assertEqual(parsed['unit'], unit)
            self.assertEqual(parsed['ops'], [100.] * 7)
            self.assertEqual(parsed['overs'], [[v] * 7 for v in [10., 2., 2., 2., 2., 2.]])

    def test_custom_window_and_larger_slice(self):
        for extra, count in [(('-f', 'Nov 23 2026 23:59:30', '-d', '0:00:30'), 3), (('-t', '30'), 2)]:
            parsed, _ = self.run_case(extra=extra)
            self.assertEqual(parsed['ops'], [100.] * count)

    def test_reset_and_unit_change_behavior(self):
        parsed, _ = self.run_case(reset=True)
        self.assertEqual(parsed['resets'], [3])
        self.assertEqual(parsed['ops'], [100., 100., 100., 0., 100., 100., 100.])
        parsed, raw = self.run_case(switch=True)
        self.assertEqual(parsed['ops'], [100.] * 3)
        self.assertIn('latency units have changed', raw)

    def test_incomplete_dump_and_absent_histogram(self):
        parsed, _ = self.run_case(incomplete=True)
        self.assertEqual(parsed['ops'], [100.] * 6)
        parsed, _ = self.run_case(histograms=('{test}-write',), expected_exit=255)
        self.assertEqual(parsed['status'], 'not-present')


if __name__ == '__main__':
    unittest.main()
