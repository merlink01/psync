# Copyright 2006 Uberan - All Rights Reserved

import time

import sql

from decorators import decorator, decorator_with_args, into

from Actor import Actor
from Future import Future
from Record import Record
from RunTimer import RunTimer
from SqlDb import SqlDb

class Clock:
    def unix(_):
        return time.time()

def groupby_list(vals, key = None):
    group_by_key = {}
    for val in vals:
        group = group_by_key.setdefault(key(val), [])
        group.append(val)
    return group_by_key

def groupby_set(vals, key = None):
    group_by_key = {}
    for val in vals:
        group = group_by_key.setdefault(key(val), set())
        group.add(val)
    return group_by_key

# like dct.setdefault(key, get_val(*args, **kargs)), except lazy
def setdefault(dct, key, get_val, *args, **kargs):
    try:
        return dct[key]
    except KeyError:
        val = get_val(*args, **kargs)
        dct[key] = val
        return val
    
def partition(vals, predicate):
    trues, falses = [], []
    for val in vals:
        (trues if predicate(val) else falses).append(val)
    return trues, falses
