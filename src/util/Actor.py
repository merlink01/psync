# Copyright 2006 Uberan - All Rights Reserved

import sys
import Queue

from util import Record, Future

class ActorStopped(Exception):
    pass

class ActorCall(Record("name", "args", "kargs", "future")):
    pass

class Actor:
    def __init__(self, name, slog):
        self.name = name
        self.slog = slog
        self.mailbox = Queue.Queue()
        self.stopped = Future()
        self.finished = Future()

    def send(self, call):
        self.mailbox.put(call, block = False)

    def run(self):
        try:
            while not self.stopped.is_set():
                try:
                    call = self.mailbox.get(block = True, timeout = 0.1)
                except Queue.Empty:
                    pass  # Try reading again.
                else:
                    method = getattr(self, call.name)
                    call.future.call(method.sync,
                                     self, *call.args, **call.kargs)
        except ActorStopped:
            self.stopped.set()
        except Exception as err:
            _, _, trace = sys.exc_info()
            self.slog.thread_died(self.name, err, trace)
        finally:
            self.finished.set()

    def stop(self):
        self.stopped.set()
        return self.finished

def async(method):
    def async_method(self, *args, **kargs):
        future = Future()
        self.send(ActorCall(method.__name__, args, kargs, future))
        return future

    async_method.sync = method
    async_method.__name__ = "async_" + method.__name__
    return async_method
