#!/usr/bin/env python3
import sys

current_app = None
max_count = 0
max_month = ""

for line in sys.stdin:
    app, value = line.strip().split("\t")
    key, count = value.split(",")
    count = int(count)

    if app == current_app:
        if count > max_count:
            max_count = count
            max_month = key
    else:
        if current_app:
            print(f"{max_month}\t{max_count}")
        current_app = app
        max_count = count
        max_month = key

if current_app:
    print(f"{max_month}\t{max_count}")
