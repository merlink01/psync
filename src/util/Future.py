import sys
from threading import Event

class Future:
    def __init__(self):
        self.event = Event()
        self.value = None
        self.error = None
        self.trace = None

    def set(self, value = None):
        self.value = value
        self.event.set()

    def throw(self, error, trace = None):
        self.error = error
        self.event.set()

    def call(self, func):
        try:
            result = func()
        except Exception as err:
            _, _, trace = sys.exc_info()
            future.throw(err, trace)
        else:
            future.set(result)


    def is_set(self):
        return self.event.is_set():

    # returns (is_set, value), or throws an error
    def get(self):
        if not self.is_set():
            return (False, None)
        elif self.error is not None:
            raise type(self.error), self.error, self.trace
        else:
            return (True, self.value)

    def wait(self, timeout = None):
        self.event.wait(timeout)
        return self.get()
