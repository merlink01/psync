import sys
from threading import Event

from Record import Record

class MockFuture(Record("val")):
    def is_set(self):
        return True

    def get(self):
        return self.val

    def wait(self):
        return self.val

    def then(self, listener):
        listener(self)
    
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

    # listener is future -> ()
    def then(self, listener):
        self.listeners.append(listener)


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
        self.vals = None
        self.listeners = []

    def is_set(self):
        return self.vals is not None

    def get(self):
        return self.vals

    def wait(self, timeout = None):
        vals = []
        for future in self.futures:
            val = future.wait(timeout)
            if not future.is_set():
                return None
            else:
                vals.append(val)
        self.vals = val
        return vals

    def then(self, listener):
        raise NotImplementedError

