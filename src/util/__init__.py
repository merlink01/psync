import sql

from Actor import Actor
from Clock import Clock
from Future import Future
from Record import Record

def groupby(vals, key = None):
    group_by_key = {}
    for val in vals:
        group = group_by_key.setdefault(key(val), [])
        group.append(val)
    return group_by_key
