# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

from contextlib import contextmanager

import hashlib
import sqlite3

from fs import (FileSystem, PathFilter, RevisionStore,
                join_paths, scan_and_update_history)
from history import (HistoryStore, MergeAction, MergeActionType,
                     calculate_merge_actions)
from util import Record, Clock, MockFuture, RunTime, SqlDb, groupby
                  
class StatusLog(Record("clock")):
    def time(self, name):
        return RunTime(name, self.clock, self.log_run_time)

    def log_run_time(self, rt):
        print ("timed", "{0:.2f} secs".format(rt.elapsed),
               rt.name, rt.result)

    def path_error(self, err):
        print ("path error", err)

    def ignored_paths(self, paths):
        for path in paths:
            print ("ignored", path)

    def not_a_file(self, path):
        print ("not a file", path)

    def could_not_hash(self, path):
        print ("could not hash", path)

    def inserted_history(self, entries):
        for entry in entries:
            print ("inserted", entry)

    def merged(self, action):
        print ("merged", action)

    @contextmanager
    def hashing(self, path):
        print ("begin hashing", path)
        yield
        print ("end hashing", path)

    @contextmanager
    def copying(self, from_path, to_path):
        print ("begin copy", from_path, to_path)
        yield
        print ("end copy", from_path, to_path)

    @contextmanager
    def trashing(self, entry):
        details = entry.hash or (entry.size, entry.mtime)
        print ("begin trashing", entry.path, details)
        yield
        print ("end trashing", entry.path, details)

# fetch is entry -> future(fetched_path)
# trash is (full_path, entry) -> ()
# merge is action -> ()
# *** handle errors
# *** better created! and changed! errors
# *** futher optimize by moving instead of copying
def diff_fetch_merge(fs, source_root, source_entries,
                     dest_root, dest_entries, dest_store,
                     fetch, trash, merge, slog):
    def verify_stat(fs_root, path, latest):
        full_path = join_paths(fs_root, path)

        if latest is None or latest.deleted:
            if fs.exists(full_path):
                raise Exception("file created", path)
        else:
            if not fs.stat_eq(full_path, latest.size, latest.mtime):
                raise Exception("file changed!", full_path,
                                "expected", (latest.size, latest.mtime),
                                "actual", (current_size, current_mtime))

        return full_path
        
    actions = calculate_merge_actions(source_entries, dest_entries)
    action_by_type = groupby(actions, MergeAction.get_type)
    touches, copies, moves, deletes, updates, update_histories, conflicts = \
             (action_by_type.get(type, []) for type in MergeActionType)

    # We must do copy actions first, in case we change the source
    # (especially if we delete in, as in the case of a move/rename.
    for action in copies:
        source_latest = next(iter(action.details))
        source_path = verify_stat(dest_root, source_latest.path, source_latest)
        dest_path = verify_stat(dest_root, action.path, action.older)
        with slog.copying(source_path, dest_path):
            fs.copy(source_path, dest_path)
            fs.touch(dest_path, action.newer.mtime)
        merge(action)
        
    for action in update_histories:
        merge(action)

    for action in touches:
        dest_path = verify_stat(dest_root, action.path, action.older)
        fs.touch(dest_path, action.newer.mtime)
        merge(action)

    for action in deletes:
        dest_path = join_paths(dest_root, action.path)
        if fs.exists(dest_path):
            dest_path = verify_stat(dest_root, action.path, action.older)
            trash(dest_path, action.older)
        merge(action)

    for action in updates:
        def copy_and_merge(source_path_f):
            source_path = source_path_f.get()
            dest_path = verify_stat(dest_root, action.path, action.older)
            trash(dest_path, action.older)
            with slog.copying(source_path, dest_path):
                fs.copy(source_path, dest_path)
                fs.touch(dest_path, action.newer.mtime)
            merge(action)
        
        fetch(action.newer).then(copy_and_merge)


# python latus.py ../test1 pthatcher@gmail.com/test1 ../test2 pthatcher@gmail.com/test2
if __name__ == "__main__":
    import os

    clock = Clock()
    slog = StatusLog(clock)
    fs = FileSystem(slog)

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

    fs_root1, peerid1, fs_root2, peerid2 = sys.argv[1:5]
    db_path1 = os.path.join(fs_root1, ".latus/db")
    db_path2 = os.path.join(fs_root2, ".latus/db")
    revisions_root2 = os.path.join(fs_root2, ".latus/revisions/")

    fs.create_parent_dirs(db_path1)
    fs.create_parent_dirs(db_path2)
    with sqlite3.connect(db_path1) as db1, sqlite3.connect(db_path2) as db2:
        history_store1 = HistoryStore(SqlDb(db1), slog)
        history_store2 = HistoryStore(SqlDb(db2), slog)
        revisions2 = RevisionStore(fs, revisions_root2)


        history_entries1 = scan_and_update_history(
            fs, fs_root1, path_filter, hash_type,
            history_store1, peerid1, clock, slog)
        history_entries2 = scan_and_update_history(
            fs, fs_root2, path_filter, hash_type,
            history_store2, peerid2, clock, slog)

        def fetch(entry):
            return MockFuture(join_paths(fs_root1, entry.path))

        def trash(source_path, dest_entry):
            if dest_entry not in revisions2:
                with slog.trashing(dest_entry):
                    revisions2.move_in(source_path, dest_entry)
                    fs.remove_empty_parent_dirs(source_path)

        def merge(action):
            new_entry = action.newer.alter(utime=clock.unix(), peerid=peerid2)
            history_store2.add_entries([new_entry])
            slog.merged(action)

        diff_fetch_merge(fs, fs_root1, history_entries1,
                         fs_root2, history_entries2, history_store2,
                         fetch, trash, merge, slog)

    # # *** use filter_entries_by_path
    # def filter_entries_by_path(entries, path_filter):
    #     return (entry for entry in entries
    #             if not path_filter.ignore_path(entry.path))

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

