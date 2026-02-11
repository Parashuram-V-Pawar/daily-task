#!/usr/bin/env python3

import sys

current_app = None
total = 0

for line in sys.stdin:
        app, count = line.strip().split("\t")
        count = int(count)

        if app == current_app:
            total += count
        else:
            if current_app:
                print(f"{current_app}\t{total}")
            current_app = app
            total = count

if current_app:
    print(f"{current_app}\t{total}")