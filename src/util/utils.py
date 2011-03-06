# Copyright (c) 2011, Peter Thatcher
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   2. Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   3. The name of the author may not be used to endorse or promote products
#      derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This is semi-random collection of utilities that I find I need all
# of the time.

import threading
import time
    
# Using time.time throught the code is a real mess for testing.
# Abstract that away.
class Clock:
    def unix(_):
        return int(time.time())

    def unix_fine(_):
        return time.time()

def groupby(vals, key = None, into = None):
    """Returns {key: values}, where values are grouped by key and put
    into the data structure given by the arg 'into', or a list if none
    given."""
    group_by_key = {}
    for val in vals:
        group = group_by_key.setdefault(key(val), [])
        group.append(val)

    if into is not None:
        for key in group_by_key:
            group_by_key[key] = into(group_by_key[key])

    return group_by_key

def setdefault(dct, key, get_val, *args, **kargs):
    """Like dct.setdefault(key, get_val(*args, **kargs)), except lazy."""
    try:
        return dct[key]
    except KeyError:
        val = get_val(*args, **kargs)
        dct[key] = val
        return val
    
def partition(vals, predicate):
    """An faster way of saying
       ([val for val in vals where predicate(val)],
        [val for val in vals where not predicate(val)])"""
    trues, falses = [], []
    for val in vals:
        (trues if predicate(val) else falses).append(val)
    return trues, falses

def flip_dict(dct):
    """Turn {key: value} into {val: key}."""
    return dict((v, k) for (k, v) in dct.iteritems())

def start_thread(func, name = None, isdaemon = True):
    thread = threading.Thread(target = func)
    if name is not None:
        thread.setName(name)
    thread.setDaemon(isdaemon)
    thread.start()
    return thread


