#!/usr/bin/env python3
"""Run the tool exactly as the webapp does and report rows the parser can't read."""
import glob
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "webapp"))
TOOL = os.path.join(HERE, "..", "asloglatency", "log_latency.py")
ROW_RE = re.compile(r"^\s*(\d{2}:\d{2}:\d{2})\s+(\d+)\s+((?:-?[\d.]+\s*)+)$")

for log in sorted(glob.glob(sys.argv[1])):
    node = re.search(r"logs_(\d+_\d+_\d+_\d+)_", log)
    node = node.group(1).replace("_", ".") if node else log
    hists = set()
    with open(log, errors="replace") as fh:
        for line in fh:
            found = re.search(r"histogram dump: (\S+)", line)
            if found:
                hists.add(found.group(1))
    for hist in sorted(hists):
        cmd = [sys.executable, TOOL, "-l", log, "-h", hist, "-t", "10", "-n", "6", "-e", "1", "-f", "head"]
        out = subprocess.run(cmd, capture_output=True, text=True).stdout
        for number, line in enumerate(out.splitlines(), 1):
            row = ROW_RE.match(line)
            if not row:
                continue
            for token in row.group(3).split():
                try:
                    float(token)
                except ValueError:
                    print("%s  %s  line %d" % (node, hist, number))
                    print("   raw   : %r" % line)
                    print("   token : %r" % token)
                    break
            else:
                continue
            break
