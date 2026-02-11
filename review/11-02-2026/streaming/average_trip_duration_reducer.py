#!/usr/bin/env python3
import sys

current_app = None
total = 0
count = 0

for line in sys.stdin:
    app, value = line.strip().split("\t")
    value = float(value)

    if current_app == app:
        total += value
        count += 1
    else:
        if current_app:
            print(f"{current_app}\t{(total/count)}")
        current_app = app
        total = value
        count = 1

if current_app:
    print(f"{current_app}\t{(total/count)}")
