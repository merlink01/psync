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
