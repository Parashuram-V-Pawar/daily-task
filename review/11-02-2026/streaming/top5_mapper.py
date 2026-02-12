#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for cols in reader:
    try:
        if len(cols) < 5:
            continue

        if cols[1] == "trip_status":
            continue

        status = cols[1].strip().upper()
        driver = cols[4].strip()

        if status == "COMPLETED":
            print(f"{driver}\t1")

    except Exception:
        continue
