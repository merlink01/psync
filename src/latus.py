# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

from contextlib import contextmanager

import hashlib
import sqlite3

from fs import (FileSystem, PathFilter, FileHistoryStore, FileHistoryEntry,
                FileScanner, join_paths,
                latest_history_entry, group_history_by_path)
from util import (Record, Enum, Clock, RunTime, SqlDb,
                  setdefault, partition)
                  
MergeAction = Enum("touch", "copy", "move", "delete", "update", "meta_update")

#*** time these
# yields [(MergeAction, source_entry, details)]
def get_merge_actions(source_entries, dest_entries):
    source_history_by_path = group_history_by_path(source_entries)
    dest_history_by_path = group_history_by_path(dest_entries)
    dest_latests_by_hash = \
            latests_by_hash_from_history_by_path(dest_history_by_path)

    for diff, path, source_latest, dest_latest in \
            diff_histories(source_history_by_path, dest_history_by_path):
        if diff != HistoryDiff.insync:
            print (diff, path, source_latest, dest_latest)
        if diff == HistoryDiff.newer:
            old = dest_latest
            new = source_latest
            if new.deleted:
                yield (MergeAction.delete, new, None)
            elif (old != None and entries_contents_match(old, new)):
                yield (MergeAction.touch, new, None)
            elif new.hash in dest_latests_by_hash:
                # TODO: futher optimize by moving instead of copying
                yield (MergeAction.copy, new, dest_latests_by_hash[new.hash])
            else:
                yield (MergeAction.update, new, None)
        elif diff == HistoryDiff.meta_conflict:
            if source_latest.deleted:
                yield (MergeAction.meta_update, source_latest, None)
            else:
                yield (MergeAction.touch, source_latest, None)
        else:
            # TODO: deal with conflicts
            pass

def latests_by_hash_from_history_by_path(history_by_path):
    latests_by_hash = {}
    for path, history in history_by_path.iteritems():
        latest = latest_history_entry(history)
        if not latest.deleted:
            setdefault(latests_by_hash, latest.hash, set).add(latest)
    return latests_by_hash

HistoryDiff = Enum("insync", "newer", "older", "conflict", "meta_conflict")

# yield (diff, path, latest1, latest2)
def diff_histories(history_by_path1, history_by_path2):
    for path1, history1 in history_by_path1.iteritems():
        history2 = history_by_path2.get(path1)
        latest1 = latest_history_entry(history1)
        latest2 = None if history2 is None else latest_history_entry(history2)
        if latest2 is None:
            diff = HistoryDiff.newer
        elif entries_match(latest1, latest2):
            diff = HistoryDiff.insync
        elif entries_contents_match(latest1, latest2):
            diff = HistoryDiff.meta_conflict
        elif has_matching_entry(history1, latest2):
            diff = HistoryDiff.newer
        elif has_matching_entry(history2, latest1):
            diff = HistoryDiff.older
        else:
            diff = HistoryDiff.conflict

        yield (diff, path1, latest1, latest2)

    for path2, history2 in history_by_path2.iteritems():
        history1 = history_by_path1.get(path2)
        if history1 is None:
            yield (HistoryDiff.older, path2,
                   None, latest_history_entry(history2))

def entries_contents_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.hash  == entry2.hash)

def entries_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.mtime == entry2.mtime and
            entry1.hash  == entry2.hash and
            entry1.author_peerid == entry2.author_peerid and
            entry1.author_utime == entry2.author_utime)
        
def has_matching_entry(history, entry):
    return any(entries_match(entry, entry2) for entry2 in history)

def filter_entries_by_path(entries, path_filter):
    return (entry for entry in entries
            if not path_filter.ignore_path(entry.path))

def first(itr):
    for val in itr:
        return val

if __name__ == "__main__":
    import time
    import os

    fs_root = sys.argv[1]
    peerid = sys.argv[2]
    db_path = os.path.join(fs_root, ".latus/db")

    fs_root2 = sys.argv[3]
    peerid2 = sys.argv[4]
    db_path2 = os.path.join(fs_root2, ".latus/db")

    class StatusLog(Record("clock")):
        def time(self, name):
            return RunTime(name, self.clock, self.log_run_time)

        def log_run_time(self, rt):
            print ("timed", "{0:.2f} secs".format(rt.elapsed),
                   rt.name, rt.result)

        def ignored_paths(self, paths):
            for path in paths:
                print ("ignored", path)

        @contextmanager
        def hashing(self, path):
            print ("begin hashing", path)
            yield
            print ("end hashing", path)
                
        def not_a_file(self, path):
            print ("not a file", path)

        def could_not_hash(self, path):
            print ("could not hash", path)

        def inserted_history(self, entries):
            for entry in entries:
                print ("inserted", entry)

    fs = FileSystem()
    clock = Clock()
    slog = StatusLog(clock)
    scanner = FileScanner(fs, clock, slog)

    hash_type = hashlib.sha1

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
        history_store = FileHistoryStore(db, slog)

        scanner.scan_and_update_history(
            fs_root, path_filter, hash_type, history_store, peerid)
        history_entries1 = history_store.read_entries(peerid)

    fs.create_parent_dirs(db_path2)
    with sqlite3.connect(db_path2) as db_conn:
        db = SqlDb(db_conn)
        history_store = FileHistoryStore(db, slog)

        scanner.scan_and_update_history(
            fs_root2, path_filter, hash_type, history_store, peerid2)
        history_entries2 = history_store.read_entries(peerid2)

        # ********************************** #
        # TODO: use filter_entries_by_path
        actions = get_merge_actions(history_entries1, history_entries2)
        copies, actions = partition(actions,
                                    lambda action: action[0] == MergeAction.copy)
        # We must do copy actions first, in case we change the source
        # (especially if we delete in, as in the case of a move/rename.
        for _, entry, local_sources in copies:
            # TODO: We could try other local sources if one fails.
            source_path = join_paths(fs_root2, first(local_sources).path)
            dest_path = join_paths(fs_root2, entry.path)
            # ***: verifying dest hasn't changed
            # *** handle errors
            print ("copying {0} => {1}", source_path, dest_path)
            # **** insert new entry!
            fs.copy_tree(source_path, dest_path)
            history_store.add_entries(
                [entry.alter(utime=clock.unix(), peerid=peerid2)])

        for action, entry, _ in actions:
            print (action, entry)
            source_path = join_paths(fs_root, entry.path)
            dest_path = join_paths(fs_root2, entry.path)
            if action == MergeAction.meta_update:
                print ("meta-merging {0}".format(dest_path))
                history_store.add_entries(
                    [entry.alter(utime=clock.unix(), peerid=peerid2)])
            elif action == MergeAction.touch:
                # ***: verifying dest hasn't changed
                # *** handle errors
                print ("touching {0}".format(dest_path))
                fs.touch(dest_path, entry.mtime)
                history_store.add_entries(
                    [entry.alter(utime=clock.unix(), peerid=peerid2)])
            elif action == MergeAction.update:
                # ***: verifying dest hasn't changed
                # *** handle errors
                # ***: implement fetching
                print ("copying {0} => {1}".format(source_path, dest_path))
                fs.copy_tree(source_path, dest_path)
                # ****: This touching doesn't seem to be working.
                fs.touch(dest_path, entry.mtime)
                history_store.add_entries(
                    [entry.alter(utime=clock.unix(), peerid=peerid2)])
            elif action == MergeAction.delete:
                # ***: verifying dest hasn't changed
                # *** handle errors
                print ("moving to trash {0}".format(dest_path))
                fs.move_to_trash(dest_path, dest_path)
                history_store.add_entries(
                    [entry.alter(utime=clock.unix(), peerid=peerid2)])
            else:
                print ("warning! don't know how to merge", action, source.path)

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

