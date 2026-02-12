#!/usr/bin/env python3

import sys

current_driver = None
total = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    driver, count = line.split("\t")
    count = int(count)

    if current_driver == driver:
        total += count
    else:
        if current_driver is not None:
            print(f"{current_driver}\t{total}")
        current_driver = driver
        total = count

if current_driver is not None:
    print(f"{current_driver}\t{total}")
