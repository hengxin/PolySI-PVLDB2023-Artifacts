#!/bin/python

from pathlib import Path

params = list(Path('result').glob('*'))

for p in params:
    total_time = 0
    count = 0

    for r in p.glob('*'):
        with open(r) as f:
            count += 1
            t = list(filter(lambda l: l.startswith('real'), f.readlines()))[0]
            total_time += float(t[t.find('m')+1:-2]) * 1000

    print('{}: avg time {:.2f}ms'.format(p, total_time / count))
