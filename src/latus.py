# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

import hashlib
import sqlite3

from fs import FileSystem, PathFilter, FileHistoryStore, FileScanner, latest_history_entry
from util import Record, Clock, RunTimer

def diff_histories(entries1, entries2):
    # *** filter entries2?
    history_by_path1 = group_history_by_path(entries1)
    history_by_path2 = group_history_by_path(entries2)

    for path1, history1 in history_by_path1.itervalues():
        history2 = history_by_path2.get(path1)
        if history2 is None:
            pass  # 1 is new
        else:
            latest1 = latest_history_entry(history1)
            latest2 = latest_history_entry(history1)
            if entries_match(latest1, latest2):
                pass  # in sync
            elif history_has_matching_entry(history1, latest2):
                pass  # 1 is newer
            elif history_has_matching_entry(history2, latest1):
                pass  # 2 is newer
            else:
                pass  # in conflict

    for path2, history2 in history_by_path2.itervalues():
        history1 = history_by_path1.get(path2)
        if history1 is None:
            pass  # 2 is new

def group_history_by_path(history_entries):
    return groupby(history_entries, FileHistoryEntry.get_path)

def entries_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.mtime == entry2.mtime and
            entry1.hash  == entry2.hash and
            entry1.author_peerid = entry2.author_peerid and
            entry1.author_utime = entry2.author_utime)
        
def history_has_matching_entry(history, entry):
    return any(entries_match(entry, entry2) for entry2 in history)


if __name__ == "__main__":
    import time

    fs_root = sys.argv[1]
    db_path = sys.argv[2]

    def log_run_time(rt):
        print ("timed", rt.name, "{0:.2f} secs".format(rt.elapsed))

    fs = FileSystem()
    clock = Clock()
    run_timer = RunTimer(clock, logger = log_run_time)
    scanner = FileScanner(fs, clock, run_timer)

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
 
    path_filter = PathFilter(globs_to_ignore, names_to_ignore)   

    with sqlite3.connect(db_path) as db_conn:
        history_store = FileHistoryStore(db_conn)

        while True:
            scanner.scan_and_update_history(fs_root, path_filter, hash_type, history_store)
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

