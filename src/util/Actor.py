# Copyright 2006 Uberan - All Rights Reserved

import logging
import Queue

from util import Record, decorator, start_thread

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

@decorator
def async(method, self, *args, **kargs):
    future = Future()
    self.send((method.__name__, args, kargs, future))
    return future
