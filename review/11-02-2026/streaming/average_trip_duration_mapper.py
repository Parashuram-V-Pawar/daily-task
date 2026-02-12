#!/usr/bin/env python3
import sys
import csv

reader = csv.reader(sys.stdin)

for fields in reader:

    if len(fields) < 10:
        continue

    if fields[0] == "trip_completed_at":
        continue

    try:
        app = fields[2].strip()
        time_str = fields[9].strip()

        h, m, s = map(int, time_str.split(":"))
        total_seconds = h*3600 + m*60 + s

        print(f"{app}\t{total_seconds}")

    except:
        continue
