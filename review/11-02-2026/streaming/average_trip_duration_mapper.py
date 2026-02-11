#!/usr/bin/env python3

import sys
import csv

for line in sys.stdin:
    fields = line.strip().split(",")

    if fields[0] == 'trip_completed_at':
        continue

    if len(fields) > 9:
        app = fields[2].strip()
        try:
            trip_time = float(fields[9].strip())
            print(f'{app}\t{trip_time}')
        except:
            continue