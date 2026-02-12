#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    key, count = parts

    if "_" not in key:
        continue

    app = key.split("_")[0]

    print(f"{app}\t{key},{count}")
