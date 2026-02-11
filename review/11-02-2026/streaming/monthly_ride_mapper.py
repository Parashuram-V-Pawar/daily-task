#!/usr/bin/env python3

import sys
import csv

data = csv.reader(sys.stdin)

for cols in data:
    if cols[7] == 'trip_start_time':
        continue

    app = cols[2].strip()
    trip_start_time = cols[7].strip()

    year_month = trip_start_time[:7]

    print(f"{app}_{year_month}\t1")



