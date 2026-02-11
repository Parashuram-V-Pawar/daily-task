#!/usr/bin/env python3
import sys

for line in sys.stdin:
    key, count = line.strip().split("\t")
    app = key.split("_")[0]
    print(f"{app}\t{key},{count}")
