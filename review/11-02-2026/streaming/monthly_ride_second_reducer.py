#!/usr/bin/env python3
import sys

current_app = None
max_count = 0
max_month = ""

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    app, value = parts

    if "," not in value:
        continue

    key, count = value.split(",")
    count = int(count)

    if "_" not in key:
        continue

    month = key.split("_")[1]

    if app == current_app:
        if count > max_count:
            max_count = count
            max_month = month
    else:
        if current_app:
            print(f"{current_app}\t{max_month}\t{max_count}")
        current_app = app
        max_count = count
        max_month = month

if current_app:
    print(f"{current_app}\t{max_month}\t{max_count}")
