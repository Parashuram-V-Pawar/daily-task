#!/usr/bin/env python3

import sys
import csv

reader = csv.reader(sys.stdin)

for cols in reader:
    if len(cols) < 10 or cols[1] == "trip_status":
        continue
    status = cols[1].strip()
    app = cols[2].strip()

    if status.upper() == "COMPLETED":
        print(f"{app}\t1")