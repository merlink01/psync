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
        self.notify_listeners()

    def throw(self, error, trace = None):
        self.error = error
        self.event.set()
        self.notify_listeners()

    def call(self, func, *args, **kargs):
        try:
            result = func(*args, **kargs)
        except Exception as err:
            _, _, trace = sys.exc_info()
            self.throw(err, trace)
        else:
            self.set(result)

    def notify_listeners(self):
        for listener in self.listeners:
            # TODO: error handling?
            listener(self)

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
