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
                call = self.wait_for_call()
                if call is not None:
                    self.handle_call(call)
        except ActorStopped:
            self.stopped.set()
        except Exception as err:
            _, _, trace = sys.exc_info()
            self.slog.actor_died(self.name, err, trace)
        finally:
            self.finish()
            self.finished.set()
            self.slog.actor_finished(self.name)

    def stop(self):
        self.stopped.set()
        return self.finished

    # Good to override for periodic events.
    def wait_for_call(self, timeout = 0.1):
        try:
            return self.mailbox.get(block = True, timeout = 0.1)
        except Queue.Empty:
            return None

    def handle_call(self, call):
        sync_method = getattr(self, call.name).sync
        call.future.call(sync_method, self, *call.args, **call.kargs)
        # TODO: Better error message if not an async method.

    # Good to override for cleanup.
    def finish(self):
        pass

def async(method):
    def async_method(self, *args, **kargs):
        future = Future()
        self.send(ActorCall(method.__name__, args, kargs, future))
        return future

    async_method.sync = method
    async_method.__name__ = "async_" + method.__name__
    return async_method
