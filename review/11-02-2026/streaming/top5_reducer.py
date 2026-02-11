#!usr/bin/env python3

import sys

current_driver = None
total = 0


for line in sys.stdin:
    driver, count = line.strip().split("\t")
    count = int(count)

    if current_driver == driver:
        total += count
    else:
        if current_driver:
            print(f"{current_driver}\t{total}")

if current_driver is not None:
    print(f"{current_driver}\t{total}")

