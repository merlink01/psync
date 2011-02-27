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
        name, args, kargs, future = call
        method = getattr(self, name)
        try:
            method = getattr(method, "sync_method") or method
            result = method(self, *args, **kargs)
            if future:
                future.set(result)
        except:
            _, err, trace = sys.exc_info()
            if future:
                future.throw(err, trace)
            else:
                self.slog.actor_error(self.name, err, trace)

    # Good to override for cleanup.
    def finish(self):
        pass

def async(method):
    return async_method(method, wants_result = False)

def async_result(method):
    return async_method(method, wants_result = True)

def async_method(method, wants_result):
    def async_method(actor, *args, **kargs):
        future = kargs.get("future")
        if future is None and wants_result:
            future = Future()
        actor.send(ActorCall(method.__name__, args, kargs, future))
        return future

    async_method.sync_method = method
    async_method.__name__ = "async_" + method.__name__
    return async_method



