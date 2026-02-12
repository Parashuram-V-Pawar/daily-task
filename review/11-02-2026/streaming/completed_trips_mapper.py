#!/usr/bin/env python3

import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    try:
        # Skip header
        if row[0] == "trip_completed_at":
            continue

        # Ensure row has enough columns
        if len(row) < 3:
            continue

        status = row[1].strip()
        app = row[2].strip()

        if status.lower() == "completed":
            print(f"{app}\t1")

    except Exception:
        continue
