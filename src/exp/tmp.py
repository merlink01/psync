import argparse

parser = argparse.ArgumentParser()
parser.add_argument("password")
args = parser.parse_args()

import logging
logging.basicConfig(level = logging.INFO,
                    format = "%(levelname)-8s %(message)s")

def main_actor_test():
    from util import Actor, async, async_result, AllFuture

    class ThreadedActorStarter:
        def __init__(self):
            self.actors = []

        def start(self, actor):
            start_thread(actor.run, actor.name)
            self.actors.append(actor)
            return actor

        def stop_all(self):
            return AllFuture([actor.stop() for actor in self.actors])

    class StatusLogActor(Actor, StatusLog):
        def __init__(self, name, clock):
            StatusLog.__init__(self, clock)
            Actor.__init__(self, name, self)

        @async
        def log(self, *args):
            print args

    class Peer(Actor):
        def __init__(self, peerid, slog):
            self.peerid = peerid
            Actor.__init__(self, repr(peerid), slog)

        def __repr__(self):
            return "{0.__class__.__name__}({0.peerid})".format(self)

        @async
        def scan(self):
            self.slog.log("scan", self.peerid)

        @async_result
        def read_entries(self):
            return [(self.peerid, "entry1")]

        @async_result
        def read_chunk(self, hash, loc, size):
            return ("chunk", self.peerid, hash, loc, size)

        def finish(self):
            self.slog.log("finish", self.peerid)

    clock = Clock()
    starter = ThreadedActorStarter()
    #slog = StatusLog(clock)
    slog = starter.start(StatusLogActor("StatusLog", clock))
    peer1 = starter.start(Peer("pthatcher@gmail.com/test1", slog))
    peer2 = starter.start(Peer("pthatcher@gmail.com/test2", slog))
    
    peer1.scan()
    peer2.scan()
    chunk1_f = peer1.read_chunk("hash", 0, 100)
    print peer2.read_chunk("hash", 100, 100).wait(0.1)
    print chunk1_f.wait(0.1)

    #import code
    #code.interact("Debug Console", local = {"peer1" : peer1, "peer2": peer2})
    starter.stop_all().wait(0.2)

def main_run_sockets():
    import socket
    sock = socket.socket()
    sock.connect(("localhost", 8080))
    stream = sock.makefile()

    # class ActorProxy(Actor):
    #     def __init__(self, name, slog, sync_names = [], async_names = []):
    #         for async_name in self.sync_names:
    #             setattr(self, async_name,
    #                     async(unbind_method(getattr(fs, async_name))))
    #         for sync_name in self.async_names:
    #             setattr(self, sync_name,
    #                     unbind_method(getattr(fs, async_name)))
    #         Actor.__init__(self, name, slog)


    # def unbind_method(method):
    #     def unbound_method(self, *args, **kargs):
    #         method(*args, **kargs)
    #     return unbound_method




    # class ActorProxy(Actor):
    #     def __init__(self, name, obj, slog):
    #         for attr_name in dir(obj):
    #             attr = getattr(obj, attr_name)
    #             if inspect.ismethod(attr) and not attr_name.startswith("__"):
    #                 print attr
    #                 method = attr
    #                 def proxy_method(*args, **kargs):
    #                     return method(self, *args, **kargs)
    #                 proxy_method.__name__ = method.__name__
    #                 setattr(self, attr_name, async(proxy_method))
    #         Actor.__init__(self, name, slog)

    #     #def handle_call(self, call):
    #     #    sync_method = getattr(self, call.name).sync
    #     #    sync_method(self, *call.args, **call.kargs)


# class FileScanner(Actor):
#     def __init__(self, fs):
#         self.fs = fs

#     @async
#     def scan(self, path):
#         for (child_path, size, mtime) in self.fs.list_stats(path):
#             if self.stopped:
#                 raise ActorStopped()
#         # ...
#         # Now, how do we setup periodic things?

# class AsyncFileSystem(ActorProxy):
#     async_names = ["list", "stats", "list_stats",
#                    "read", "write", "create_dir",
#                    "move_tree", "copy_tree", "delete_tree"]
#     sync_names = ["exists", "isdir", "isempty", "stat",
#                   "touch", "move_to_trash"]
        
## possible XMPP:
# <iq type="get">
#   <files since=...>
#   <chunk hash=... loc=... size=...>
# <iq type="result">
#   <files>
#     <file path=... mtime=... size=... utime=...>
#   <chunk hash=... loc=... size=...>
#   

## For permissions, we need:
# (peerid, groupid, prefix?, can_read, can_write, is_owner)
# what is a groupid?
# do we use prefix?
# where is this stored?

## gevent server:
# from gevent.server import StreamServer
# server = StreamServer(('0.0.0.0', 6000), lambda (socket, address): ...)
# server.server_forever()
# ...
# socket.makefile().write("foo")
# socket.makefile().read()
#
# from gevent.socket import wait_write
# 

## Trying to get gevent and sleekxmpp to work together
#import gevent
# from gevent import monkey
# monkey.patch_all()
# 
# Maybe try this:
# class GeventXmpp(sleekxmpp.ClientXMPP):
#   def process(self):
#     gevent.spawn(self._event_runner)
#     gevent.spawn(self._send_thread)
#     gevent.spawn(self._process)

## From old ShareEver code
# private_ip, private_port = (Utils.get_local_ip(), config.connectionPort)
# peer = Peer(directory)
# network = Network(map_port_description = "ShareEver")
# fs = get_file_system(config.useOSTrash, config.customTrashFolder)
# log = ???

# connection_listener = ThreadedSocketServerListener(
#  "connection listener", config.connectionPort)
# rendezvous_listener = ThreadedSocketServerListener(
#   "rendezvous listener", config.rendezvousPort)
# start/stop:
#  peer, connection_listener, rendezvous_listener

# TODO: write persistent info (not config!)
#  connection port
#  peerid
#  crypto keys?
# TODO: debug consoloe: code.interact(
#  "ShareEver Debugging Console", local = {"peer" : peer}

# XMPP stuff
    # Interesting plugins:
    # xep-009: RPC
    # xep-030: Service Discovery
    # xep-050: Ad-Hoc commands
    # xep-060: PubSub
    # xep-085: Chat state
    # xep-199: Ping
    # client.registerPlugin("xep_0030")  # Service Discovery
    # client.registerPlugin("xep_0004")  # Data Forms
    # client.registerPlugin("xep_0060")  # PubSub
    # client.registerPlugin("xep_0199")  # XMPP Ping

    # To make your own plugin, you much make a stream event handler like this:
    # from https://github.com/fritzy/SleekXMPP/wiki/Event-Handlers
    # self.registerHandler(
    #     Callback('Example Handler',
    #              MatchXPath('{%s}iq/{%s}task' % (self.default_ns, Task.namespace)),
    #              self.handle_task))

    # presence/subscription handled like so:
    # https://github.com/fritzy/SleekXMPP/wiki/Stanzas:-Presence

    # creating our own stanzas:
    # https://github.com/fritzy/SleekXMPP/wiki/Stanza-Objects

def main_get_public_address():
    import re
    import urllib2

    # NOTE: Doing sock.setsockopt(socket.SOL_SOCKET,
    # socket.SO_REUSEADDR, 1) doesn't seem to work :(.
    # Also, the apple router appears to completely randomize the ports :(.
    stream = urllib2.urlopen("http://www.ipchicken.com", timeout = 5.0)
    data = stream.read() 
    address_match = re.compile(
        "(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})").search(data)
    port_match = re.compile("Port:\s*(\S+)").search(data)
    address = address_match.group(1) if address_match is not None else None
    port = port_match.group(1) if port_match is not None else None
    print address, port
    

