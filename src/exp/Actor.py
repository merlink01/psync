# Copyright (c) 2012, Peter Thatcher
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



