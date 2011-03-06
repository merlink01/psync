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

import sys
import time
from threading import Event

from Record import Record

class MockFuture(Record("val")):
    def is_set(self):
        return True

    def get(self):
        return self.val

    def wait(self):
        return self.val

class Future:
    def __init__(self):
        self.event = Event()
        self.value = None
        self.error = None
        self.trace = None
        self.listeners = []

    def is_set(self):
        return self.event.is_set()

    # returns None if not set
    def get(self, default = None):
        if self.is_set():
            if self.error is None:
                return self.value
            else:
                raise type(self.error), self.error, self.trace
        else:
            return default

    def wait(self, timeout = None):
        self.event.wait(timeout)
        return self.get()

    def set(self, value = None):
        self.value = value
        self.event.set()

    def throw(self, error, trace = None):
        self.error = error
        self.event.set()

class AllFuture:
    def __init__(self, futures):
        self.futures = futures

    def is_set(self):
        return all(future.is_set() for future in self.futures)

    def get(self):
        if is_set(self):
            return [future.get() for future in future]
        else:
            return None

    def wait(self, timeout = None):
        if timeout is None:
            return [future.wait(timeout) for future in self.futures]
        else:
            deadline = time.time() + timeout
            def time_until_deadline():
                return max(0, deadline, time.time())

            return [future.wait(time_until_deadline())
                    for future in self.futures]
