import sys
from threading import Event

from Record import Record

class MockFuture(Record("val")):
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

    def set(self, value = None):
        self.value = value
        self.event.set()
        self.notify_listeners()

    def throw(self, error, trace = None):
        self.error = error
        self.event.set()
        self.notify_listeners()

    def call(self, func):
        try:
            result = func()
        except Exception as err:
            _, _, trace = sys.exc_info()
            self.throw(err, trace)
        else:
            self.set(result)

    def notify_listeners(self):
        for listener in self.listeners:
            # TODO: error handling?
            listener(self)


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
