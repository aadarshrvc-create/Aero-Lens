import datetime
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "webapp"))
import fetch as log_fetch  # noqa: E402


class TestFetchResolve(unittest.TestCase):
    def test_rotated_name(self):
        d = datetime.date(2026, 11, 15)
        self.assertEqual(log_fetch.rotated_basename(d, 1), "aerospike.log-20261116.gz")

    def test_retention(self):
        today = datetime.date(2026, 11, 17)
        with self.assertRaises(ValueError):
            log_fetch.check_retention(datetime.date(2026, 10, 1), 30, today=today)

    def test_today_includes_active_log(self):
        today = datetime.date(2026, 11, 17)
        cfg = {"rotation_suffix_plus_days": 1, "log_dir": "/var/log/aerospike"}
        paths, names = log_fetch.remote_files_for_date(cfg, today, today=today)
        self.assertIn("aerospike.log-20261118.gz", names)
        self.assertTrue(any(n == "aerospike.log" for n in names))


if __name__ == "__main__":
    unittest.main()