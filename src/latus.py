# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

import hashlib
import logging
import sqlite3

from fs import FileSystem, FileHistoryStore, FileHistoryEntry, PathFilter, join_paths, mtimes_eq
from util import Record, Clock, groupby, partition

DELETED_SIZE = 0
DELETED_MTIME = 0

def group_stats_by_path(file_stats):
    return dict((path, (size, mtime)) for path, size, mtime in file_stats)

# yields (path, size, mtime) if the path is new
#        (path, size, mtime) if the size or mtime have changed
#        (path, DELETED_SIZE, DELETED_MTIME) if the path is missing
def diff_file_stats(file_stats, history_entries, path_filter, run_timer):
    with run_timer("group history by path"):
        history_by_path = groupby(history_entries, FileHistoryEntry.get_path)

    with run_timer("compare to latest history"):
        for path, size, mtime in file_stats:
            history = history_by_path.get(path)
            last = None if history is None else max(history)
            if last is not None and last.size == size and mtimes_eq(last.mtime, mtime):
                # unchanged
                pass  
            elif path_filter.ignore_path(path):
                # ignored  
                pass
                #*** improve logging here
                # print ("ignoring", path)
                # logging.warning("ignoring {0}".format(path))
            else:
                # change or created (created if last == None)
                yield path, size, mtime  

    with run_timer("find missing paths"):
        history_paths = frozenset(history_by_path.iterkeys())
        listed_paths = frozenset(path for path, size, mtime in file_stats)
        missing_paths = history_paths - listed_paths
        for path in missing_paths:
            yield path, DELETED_SIZE, DELETED_MTIME  # deleted
            
# yields new FileHistoryEntry
def hash_file_stats(fs, fs_root, file_stats, hash_type, clock):
    utime = int(clock.unix())
    for path, size, mtime in file_stats:
        full_path = join_paths(fs_root, path)

        if fs.isfile(full_path):
            try:
                hash = fs.hash(full_path, hash_type)
                yield FileHistoryEntry(path, utime, size, mtime, hash.encode("hex"))
            except IOError:
                pass
                # *** better logging
                # print ("ignoring because could not hash", path)
        else:
            pass
            # *** better logging
            # print ("ignoring because it's not a file", path)

# ***: better logging here
def scan_and_update_history(fs, fs_root, names_to_ignore, path_filter, hash_type,
                            history_store, clock, run_timer):
    with run_timer("read history"):
        history_entries = history_store.read_entries()
        print ("history entries", len(history_entries))

    with run_timer("scan files"):
        file_stats = list(fs.list_stats(fs_root, names_to_ignore))
        print ("file stats", len(file_stats))

    with run_timer("diff file stats"):
        changed_stats = list(
            diff_file_stats(file_stats, history_entries, path_filter, run_timer))
        print ("changed stats", len(changed_stats))

    with run_timer("hash files"):
        new_history_entries = list(
            hash_file_stats(fs, fs_root, changed_stats, hash_type, clock))
        print ("new history entries", len(new_history_entries))

    with run_timer("rescan files"):
        rescan_stats = list(fs.stats(fs_root, (path for path, size, mtime in changed_stats)))
        print ("rescanned file stats", len(rescan_stats))

    with run_timer("group rescanned stats"):
        rescan_stats_by_path = group_stats_by_path(rescan_stats)

    with run_timer("check change stability"):
        def is_stable(entry):
            (rescan_size, rescan_mtime) = \
                rescan_stats_by_path.get(entry.path, (DELETED_SIZE, DELETED_MTIME))
            #print ("size vs", entry.size, rescan_size)
            #print ("mtime vs", entry.mtime, rescan_mtime)
            return entry.size == rescan_size and entry.mtime == rescan_mtime

        stable_entries, unstable_entries = partition(new_history_entries, is_stable)
        print ("stable entries", len(stable_entries), len(unstable_entries))

    for entry in unstable_entries:
        #*** better logging
        print ("unstable", entry.path)

    # *** remove
    old_paths = frozenset(entry.path for entry in history_entries)
    for entry in stable_entries:
        if entry.mtime == DELETED_MTIME:
            print ("deleted", entry)
        elif entry.path not in old_paths:
            print ("added", entry)
        else:
            print ("changed", entry)

    with run_timer("insert new history entries"):
        history_store.add_entries(stable_entries)

    with run_timer("reread history"):
        history_entries = history_store.read_entries()
        history_by_path = groupby(history_entries, FileHistoryEntry.get_path)
        total_size = sum(max(history).size for history in history_by_path.itervalues())
        print ("new history", len(history_entries), len(history_by_path), total_size)

# *** move
class RunTimer:
    def __init__(self, clock):
        self.clock = clock

    def __call__(self, name):
        return RunTime(name, self.clock)

class RunTime:
    def __init__(self, name, clock):
        self.name   = name
        self.clock  = clock
        self.before = None
        self.after  = None

    def __enter__(self):
        self.before = self.clock.unix()

    def __exit__(self, *args):
        self.after = self.clock.unix()
        # *** better logging
        print ("timed", self.name, repr(self))

    @property
    def elapsed(self):
        if self.before is None or self.after is None:
            return None
        else:
            return self.after - self.before

    def __repr__(self):
        if self.before is None:
            return "never started"
        elif self.after is None:
            return "never finished"
        else:
            return "{0:.2f} secs".format(self.elapsed)

if __name__ == "__main__":
    import time

    fs_root = sys.argv[1]
    db_path = sys.argv[2]

    clock = Clock()
    run_timer = RunTimer(clock)
    fs = FileSystem()

    # hash_type = hashlib.sha1
    hash_type = None

    names_to_ignore = frozenset([
            # Mac OSX things we shouldn't sync, mostly caches and trashes
            "Library", ".Trash", "iPod Photo Cache", ".DS_Store",

            # Unix things we shouldn't sync, mostly caches and trashes
            ".m2", ".ivy2", ".fontconfig", ".thumbnails", "thumbs.db",
            ".abobe", ".dvdcss", ".cache", ".macromedia", ".xsession-errors",
            ".mozilla", ".java", ".gconf", ".gconfd", ".kde", ".nautilus", ".local",
            ".icons", ".themes",

            # These are debatable.
            ".hg", ".git", ".evolution"])

    # For 5 patterns, the initial scan time is doubled.  But, that
    # time is dwarfed by the hash time anyway.  The path filter can
    # memoize to make subsequen scans just as fast as unfiltered ones.
    globs_to_ignore = \
        [# Parallels big files we probably should never sync
         "*.hds", "*.mem",

         # Contents that change a lot, but we wouldn't want to sync
         ".config/google-chrome/*", ".config/google-googletalkplugin/*",

         # emacs temp files, which we probably never care to sync
         "*~", "*~$", "~*.tmp"]
 
    path_filter = PathFilter(globs_to_ignore)   

    with sqlite3.connect(db_path) as db_conn:
        history_store = FileHistoryStore(db_conn)

        while True:
            scan_and_update_history(fs, fs_root, names_to_ignore, path_filter, hash_type,
                                    history_store, clock, run_timer)
            time.sleep(180)  # every 3 minutes
        


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

