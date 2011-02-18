# Copyright 2006 Uberan - All Rights Reserved

import logging
import Queue

from util import Record

class ActorStopped(Exception):
    pass

class Actor:
    def __init__(self, name = None):
        self.name = name or self.__class__.__name__
        self.thread = None
        self.mailbox = Queue.Queue()
        self.stopped = None

    def start(self):
        self.thread = start_thread(self.run, self.name)
        
    def stop(self):
        self.stopped = Future()

    def send(self, call):
        self.mailbox.put(call, block = False)

    def run(self):
        try:
            while not self.stopped:
                try:
                    name, args, kargs, future = \
                          self.mailbox.get(block = True, timeout = 0.1)
                except Queue.Empty:
                    pass  # Try reading again.
                else:
                    try:
                        future.call(lambda: getattr(self, name)(*args, **kargs))
                    except Exception as err:
                        logging.warning("Ignoring error in {0}: {1}".format(
                            self.name, err))
        except ActorStopped:
            self.stopped.set()
        except Exception as err:
            logging.error("Died from error in {0}: {1}".format(
                self.name, err))

def async(method):
    name = method.__name__

    def async_method(self, *args, **kargs):
        future = Future()
        self.send((name, args, kargs, future))
        return future

    async_method.__name__ = name
    return async_method
    
class ActorProxy:
    async_names = []
    sync_names = []

    def __init__(self, fs):
        for async_name in self.async_names:
            setattr(self, async_name,
                    async(unbind_method(getattr(fs, async_name))))
        for sync_name in self.sync_names:
            setattr(self, sync_name,
                    unbind_method(getattr(fs, async_name)))

def start_thread(func, name = None, isdaemon = True):
    thread = threading.Thread(target = func)
    thread.setName(name)
    thread.setDaemon(True)
    thread.start()
    return thread
    
def unbind_method(method):
    def unbound_method(self, *args, **kargs):
        method(*args, **kargs)
    return unbound_method

