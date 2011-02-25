# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

from contextlib import contextmanager

import hashlib
import sqlite3

from fs import (FileSystem, PathFilter, RevisionStore,
                join_paths, scan_and_update_history)
from history import (HistoryStore, MergeAction, MergeActionType, MergeLog,
                     calculate_merge_actions)
from util import Record, Clock, MockFuture, RunTime, SqlDb, groupby, flip_dict
                  
class StatusLog(Record("clock")):
    def time(self, name):
        return RunTime(name, self.clock, self.log_run_time)

    def log_run_time(self, rt):
        print ("timed", "{0:.2f} secs".format(rt.elapsed),
               rt.name, rt.result)

    def path_error(self, err):
        print ("path error", err)

    def ignored_rpaths(self, rpaths):
        for rpath in rpaths:
            print ("ignored", rpath)

    def ignored_rpath_without_groupid(self, gpath):
        print ("ignore rpath without groupid", gpath)

    def ignored_gpath_without_root(self, gpath):
        print ("ignore gpath without root", gpath)

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
    def moving(self, from_path, to_path):
        print ("begin move", from_path, to_path)
        yield
        print ("end move", from_path, to_path)

    @contextmanager
    def trashing(self, entry):
        details = entry.hash or (entry.size, entry.mtime)
        print ("begin trashing", entry.path, details)
        yield
        print ("end trashing", entry.path, details)

    @contextmanager
    def untrashing(self, entry, dest_path):
        details = entry.hash or (entry.size, entry.mtime)
        print ("begin untrashing", dest_path, entry.path, details)
        yield
        print ("end untrashing", dest_path, entry.path, details)

# *** implement reading .latusconf
class Groupids(Record("root_by_groupid", "groupid_by_root")):
    def __new__(cls, root_by_groupid):
        groupid_by_root = flip_dict(root_by_groupid)
        return cls.new(root_by_groupid, groupid_by_root)

    def to_root(self, groupid):
        return self.root_by_groupid.get(groupid, None)

    def from_root(self, root):
        return self.groupid_by_root.get(root, None)


# fetch is entry -> future(fetched_path)
# trash is (full_path, entry) -> ()
# merge is action -> ()
# *** handle errors, especially unknown groupid, created! and changed! errors
def diff_fetch_merge(source_entries, source_groupids,
                     dest_entries, dest_groupids, dest_store,
                     fetch, trash, merge, revisions, fs, slog):
    def get_dest_path(gpath):
        (groupid, path) = gpath
        root = dest_groupids.to_root(groupid)
        if root is None:
            raise Exception("unknown groupid!", groupid)
        return join_paths(root, path)

    def verify_stat(gpath, latest):
        full_path = get_dest_path(gpath)

        if latest is None or latest.deleted:
            if fs.exists(full_path):
                raise Exception("file created", full_path)
        else:
            if not fs.stat_eq(full_path, latest.size, latest.mtime):
                raise Exception("file changed!", full_path)

        return full_path
        
    actions = calculate_merge_actions(source_entries, dest_entries, revisions)
    action_by_type = groupby(actions, MergeAction.get_type)
    touches, copies, moves, deletes, undeletes, updates, uphists, conflicts = \
             (action_by_type.get(type, []) for type in MergeActionType)

    # We're going to simply resolve conflicts by letting the newer
    # mtime win.  Since a deleted mtime is 0, a non-deleted always
    # wins over a deleted.  If the remove end is older, we copy it to
    # "revisions", so that if later it "wins", it's a local copy, and
    # so a user could potentially look at it.
    for action in conflicts:
        older, newer = action.older, action.newer
        # When mtimes are equal, use utime, size, and hash as tie-breakers.
        if (newer.mtime, newer.utime, newer.size, newer.hash) \
               > (older.mtime, older.utime, older.size, older.hash):
            updates.append(action)
        else:
            # TODO: Make sure fetching goes into "revisions".
            fetch(action.newer)

    # If a copy also has a matching delete, make it as "move".
    deletes_by_hash = groupby(deletes, \
            lambda delete: delete.older.hash if delete.older else None)
    real_copies = []
    for action in copies:
        deletes_of_hash = deletes_by_hash.get(action.newer.hash, [])
        if action.newer.hash and deletes_of_hash:
            # Pop so we only match a given delete once.  But we
            # leave the deleted in the deleteds so that it's put
            # in the history and merge data, but we don't put it
            # in the revisions.
            delete = deletes_of_hash.pop()
            moves.append(action.alter(
                type = MergeActionType.move, details = delete.older))
        else:
            real_copies.append(action)
    copies = real_copies

    # We must do copy and move actions first, in case we change the souce.
    for action in copies:
        source_latest = next(iter(action.details))
        source_path = verify_stat(source_latest.gpath, source_latest)
        dest_path = verify_stat(action.gpath, action.older)
        with slog.copying(source_path, dest_path):
            fs.copy(source_path, dest_path, mtime = action.newer.mtime)
        merge(action)
        
    for action in moves:
        source_latest = action.details
        source_path = verify_stat(source_latest.gpath, source_latest)
        dest_path = verify_stat(action.gpath, action.older)
        with slog.moving(source_path, dest_path):
            fs.move(source_path, dest_path, mtime = action.newer.mtime)
        merge(action)

    for action in uphists:
        merge(action)

    for action in touches:
        dest_path = verify_stat(action.gpath, action.older)
        fs.touch(dest_path, action.newer.mtime)
        merge(action)

    for action in deletes:
        dest_path = get_dest_path(action.gpath)
        if fs.exists(dest_path):
            dest_path = verify_stat(action.gpath, action.older)
            trash(dest_path, action.older)
        merge(action)

    for action in undeletes:
        rev_entry = action.details
        dest_path = verify_stat(action.gpath, action.older)
        trash(dest_path, action.older)
        with slog.untrashing(rev_entry, dest_path):
            revisions.copy_out(rev_entry, dest_path)
        merge(action)

    for action in updates:
        def copy_and_merge(source_path_f):
            source_path = source_path_f.get()
            dest_path = verify_stat(action.gpath, action.older)
            trash(dest_path, action.older)
            with slog.copying(source_path, dest_path):
                fs.copy(source_path, dest_path, mtime = action.newer.mtime)
            merge(action)

        fetch(action.newer).then(copy_and_merge)
    
def filter_entries_by_gpath(entries, groupids, path_filter):
    return (entry for entry in entries
            if (groupids.to_root(entry.groupid) is not None and
                not path_filter.ignore_path(entry.path)))

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
    db_path1 = os.path.join(fs_root1, ".latus/latus.db")
    db_path2 = os.path.join(fs_root2, ".latus/latus.db")
    revisions_root2 = os.path.join(fs_root2, ".latus/revisions/")
    root_mark = ".latusconf"

    groupids1 = Groupids({"group1": fs_root1,
                          "group1/cmusic": os.path.join(
                              fs_root1, "Conference Music")})
    groupids2 = Groupids({"group1": fs_root2,
                          "group1/cmusic": os.path.join(
                              fs_root2, "cmusic")})

    fs.create_parent_dirs(db_path1)
    fs.create_parent_dirs(db_path2)
    with sqlite3.connect(db_path1) as db1, sqlite3.connect(db_path2) as db2:
        history_store1 = HistoryStore(SqlDb(db1), slog)
        history_store2 = HistoryStore(SqlDb(db2), slog)
        revisions2 = RevisionStore(fs, revisions_root2)
        merge_log2 = MergeLog(SqlDb(db2), clock)

        history_entries1 = scan_and_update_history(
            fs, fs_root1, root_mark, path_filter, hash_type,
            history_store1, peerid1, groupids1, clock, slog)
        history_entries2 = scan_and_update_history(
            fs, fs_root2, root_mark, path_filter, hash_type,
            history_store2, peerid2, groupids2, clock, slog)

        def fetch(entry):
            root = groupids1.to_root(entry.groupid)
            return MockFuture(join_paths(root, entry.path))

        def trash(source_path, dest_entry):
            if fs.exists(source_path):
                with slog.trashing(dest_entry):
                    revisions2.move_in(source_path, dest_entry)
                    fs.remove_empty_parent_dirs(source_path)

        def merge(action):
            new_entry = action.newer.alter(utime=clock.unix(), peerid=peerid2)
            history_store2.add_entries([new_entry])
            slog.merged(action)
            merge_log2.add_action(action.set_newer(new_entry))

        history_entries1 = filter_entries_by_gpath(
            history_entries1, groupids2, path_filter)
        diff_fetch_merge(history_entries1, groupids1,
                         history_entries2, groupids2, history_store2,
                         fetch, trash, merge, revisions2, fs, slog)

        # for log_entry in sorted(merge_log2.read_entries(peerid2)):
        #    print log_entry
