# Copyright 2006 Uberan - All Rights Reserved

import fnmatch
import hashlib
import re
import sqlite3
import sys

from fs import FileSystem, FileHistoryStore, FileHistoryEntry, join_paths, mtimes_eq
from util import Record, Clock, groupby

DELETED_SIZE = 0
DELETED_MTIME = 0

def main(fs_root, db_path):
    clock = Clock()
    fs = FileSystem()

    #hash_type = hashlib.sha1  # None
    hash_type = None
    names_to_ignore = frozenset([
            # Mac OSX things we shouldn't sync, mostly caches and trashes
            "Library", ".Trash", "iPod Photo Cache", ".DS_Store",

            # Unix things we shouldn't sync, mostly caches and trashes
            ".m2", ".ivy2", ".fontconfig", ".thumbnails",
            ".abobe", ".dvdcss", ".cache", ".macromedia",
            ".mozilla", ".java", ".gconf", ".kde", ".nautilus", ".local",
            ".icons", ".themes",

            # These are debatable.
            ".hg", ".git", ".evolution"])

    # TODO: After an initial history scan, this causes 20% increase in
    # scan time (for 5 patterns).  The first time, however, it doubles
    # the scan time.  Of course, the first time, that's dwarfed by the
    # hash time anyway.  If we really wanted to improve the performance
    # beyond that, we could try memoizing.
    patterns_to_ignore = frozenset(
        re.compile(fnmatch.translate(pattern), re.IGNORECASE) for pattern in 
        [# Parallels big files we probably should never sync
         "*.hds", "*.mem", "*.jpg",

         # emacs temp files, which we probably never care to sync
         "*~"])
 
    ignored_paths = set()
    with sqlite3.connect(db_path) as db_conn:
        history_store = FileHistoryStore(db_conn)
        
        history_entries, run_time = clock.run_time(history_store.read_entries)
        history_by_path = groupby(history_entries, FileHistoryEntry.get_path)
        print ("read history", run_time, len(history_entries), len(history_by_path))

        def new_history_entries():
            new_utime = int(clock.now_unix())
            for (change, path, size, mtime, history) in \
                    scan_and_diff(fs, fs_root, names_to_ignore, history_by_path):
                if path in ignored_paths or \
                        any(pattern.match(path) for pattern in patterns_to_ignore):
                    ignored_paths.add(path)
                    change = "ignored"

                # if change != "unchanged":
                #    print (change, path, size, mtime, history)
                # if change == "ignored":
                #     print (change, path, size, mtime, history)
                # TODO: make an enum?
                if change == "created" or change == "changed":
                    # TODO: make a FileSystem that always knows how to do prepend a path?
                    #print ("hash", path)
                    #hash = fs.hash(join_paths(fs_root, path), hash_type).encode("hex")
                    #print ("hashed", path, hash)
                    hash = ""
                    yield FileHistoryEntry(path, new_utime, size, mtime, hash)
                elif change == "deleted":
                    hash = ""
                    yield FileHistoryEntry(path, new_utime, size, mtime, hash)
        new_history_entries, run_time = clock.run_time(lambda: list(new_history_entries()))
        print ("scanned and diffed and hashed", run_time, len(new_history_entries))
        
        fs.touch(db_path, clock.now_unix())
        rescan_by_path, run_time = clock.run_time(
            lambda: dict((path, (size, mtime)) for path, size, mtime in
                         fs.stats(fs_root, (entry.path for entry in new_history_entries))))
        print ("rescanned fs", run_time, len(rescan_by_path))

        def stable_new_history_entries():
            for entry in new_history_entries:
                (rescan_size, rescan_mtime) = \
                    rescan_by_path.get(entry.path, (DELETED_SIZE, DELETED_MTIME))
                if entry.size == rescan_size and entry.mtime == rescan_mtime:
                    yield entry
                else:
                    print ("unstable", entry.path, \
                               (entry.size, rescan_size), (entry.mtime, rescan_mtime))
        stable_new_history_entries = list(stable_new_history_entries())

        _, run_time = clock.run_time(history_store.add_entries, stable_new_history_entries)
        print ("inserted", run_time, len(stable_new_history_entries))

        history_entries, run_time = clock.run_time(history_store.read_entries)
        history_by_path2 = groupby(history_entries, FileHistoryEntry.get_path)
        print ("read history again", run_time, len(history_entries), len(history_by_path2), \
                   sum(max(history).size for history in history_by_path2.itervalues()))
                      

# yields (change, path, size, mtime, history)
def scan_and_diff(fs, root, names_to_ignore, history_by_path):
    missing_paths = set(history_by_path.iterkeys())
    for path, size, mtime in fs.list_stats(root, names_to_ignore):
        missing_paths.discard(path)

        history = history_by_path.get(path)
        if history is None:
            yield ("created", path, size, mtime, history)
        else:
            latest = max(history)
            if size != latest.size or not mtimes_eq(mtime, latest.mtime):
                yield ("changed", path, size, mtime, history)
            else:
                yield ("unchanged", path, size, mtime, history)

    for path in sorted(missing_paths):
        yield ("deleted", path, DELETED_SIZE, DELETED_MTIME, history)

if __name__ == "__main__":
    root = sys.argv[1]
    db_path = sys.argv[2]

    main(root, db_path)

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
#*** put list, stats, and list stats into a list (instead of iterator)
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

