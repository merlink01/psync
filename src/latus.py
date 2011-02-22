# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

import hashlib
import sqlite3

from fs import (FileSystem, PathFilter, FileHistoryStore, FileHistoryEntry,
                FileScanner, latest_history_entry, group_history_by_path)
from util import Record, Clock, RunTimer, SqlDb
                  
# *** use enum for newer, older, insync, etc
def merge_histories(entries1, entries2):
    for path, diff, latest1, latest2 in diff_histories(entries1, entries2):
        if diff == "newer":
            if (latest2 is not None
                and latest2.size == latest1.size
                and latest2.hash == latest1.hash):
                print ("touch", path, latest2.mtime)
            if latest1.hash in ():  # FILE_SYSTEM_SOMEWHERE:
                if source_path in DELETEDS:
                    print ("move", path, "from", source_path)
                else:
                    print ("copy", path, "from", source_path)
            elif latest1.hash in ():  # TRASH_SOMEWHERE:
                print ("undelete", path, "from", source_path)
            elif latest1.deleted:
                print ("delete", path)
            elif latest2 is None:
                print ("fetch and create", path)
        elif diff == "inconflict":
            print (diff, path)  # *** fetch but don't merge
        else:
            print (diff, path)

def diff_histories(entries1, entries2):
    history_by_path1 = group_history_by_path(entries1)
    history_by_path2 = group_history_by_path(entries2)

    for path1, history1 in history_by_path1.iteritems():
        history2 = history_by_path2.get(path1)
        latest1 = latest_history_entry(history1)
        latest2 = None if history2 is None else latest_history_entry(history2)
        if latest2 is None:
            diff = "newer"
        elif entries_match(latest1, latest2):
            diff = "insync"
        elif has_matching_entry(history1, latest2):
            diff = "newer"
        elif has_matching_entry(history2, latest1):
            diff = "older"
        else:
            diff = "conflict"

        yield (path1, diff, latest1, latest2)

    for path2, history2 in history_by_path2.iteritems():
        history1 = history_by_path1.get(path2)
        if history1 is None:
            yield (path1, "older", None, latest_history_entry(history2))

def entries_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.mtime == entry2.mtime and
            entry1.hash  == entry2.hash and
            entry1.author_peerid == entry2.author_peerid and
            entry1.author_utime == entry2.author_utime)
        
def has_matching_entry(history, entry):
    return any(entries_match(entry, entry2) for entry2 in history)


if __name__ == "__main__":
    import time
    import os

    fs_root = sys.argv[1]
    peerid = sys.argv[2]
    db_path = os.path.join(fs_root, ".latus/db")

    fs_root2 = sys.argv[3]
    peerid2 = sys.argv[4]
    db_path2 = os.path.join(fs_root2, ".latus/db")


    def log_run_time(rt):
        print ("timed", "{0:.2f} secs".format(rt.elapsed), rt.name, rt.result)

    fs = FileSystem()
    clock = Clock()
    run_timer = RunTimer(clock, logger = log_run_time)
    scanner = FileScanner(fs, clock, run_timer)

    # hash_type = hashlib.sha1
    hash_type = None

    names_to_ignore = frozenset([
        # don't scan our selves!
        ".latus",

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

    fs.create_parent_dirs(db_path)
    with sqlite3.connect(db_path) as db_conn:
        db = SqlDb(db_conn)
        history_store = FileHistoryStore(db)

        scanner.scan_and_update_history(
            fs_root, path_filter, hash_type, history_store, peerid)
        history_entries1 = history_store.read_entries()

    fs.create_parent_dirs(db_path2)
    with sqlite3.connect(db_path2) as db_conn:
        db = SqlDb(db_conn)
        history_store = FileHistoryStore(db)

        scanner.scan_and_update_history(
            fs_root2, path_filter, hash_type, history_store, peerid2)
        history_entries2 = history_store.read_entries()

    # *** filter entries2?
    merge_histories(history_entries1, history_entries2)


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

