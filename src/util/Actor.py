# Copyright 2006 Uberan - All Rights Reserved

import logging
import Queue

from util import Record

class ActorStopped(Exception):
    pass

class ActorCall(Record("name", "args", "kargs", "future")):
    pass

def async(method):
    name = method.__name__

    def async_method(self, *args, **kargs):
        #***  .set(val = None), .throw(err), get(timeout = None)
        future = Future()
        self.send(ActorCall(name, args, kargs, future))
        return future

    async_method.__name__ = name
    return async_method
    

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
                        call_into_future(future, \
                            lambda: getattr(self, name)(*args, **kargs))
                    except Exception as err:
                        logging.warning("Ignoring error in {}: {}".format(
                            self.name, err))
        except ActorStopped:
            self.stopped.set()
        except Exception as err:
            logging.error("Died from error in {}: {}".format(
                self.name, err))

def start_thread(func, name = None, isdaemon = True):
    thread = threading.Thread(target = func)
    thread.setName(name)
    thread.setDaemon(True)
    thread.start()
    return thread
    
def call_into_future(future, func):
    try:
        result = func()
    except Exception as err:
        future.throw(err)
    else:
        future.set(result)

class FileScanner(Actor):
    def __init__(self, fs):
        self.fs = fs

    @async
    def scan(self, path):
        for (child_path, size, mtime) in self.fs.list_stats(path):
            if self.stopped:
                raise ActorStopped()
                
        
    
