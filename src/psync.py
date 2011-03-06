# Copyright (c) 2011, Peter Thatcher
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   2. Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   3. The name of the author may not be used to endorse or promote products
#      derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

from contextlib import contextmanager

import hashlib
import os
import sqlite3
import traceback

from fs import (FileSystem, PathFilter, RevisionStore,
                join_paths, scan_and_update_history, diff_and_merge)
from history import HistoryStore, MergeLog
from util import Record, Clock, RunTime, SqlDb, flip_dict
                  
class StatusLog:
    """ All the loggable events of the entire application are routed
    through this class.  An instance of this class is passed around in
    a lot of places as a"slog". Some of these events are "timed" events,
    which me ans that we keep track of when they started, when
    they ended, and how long the took."""

    def __init__(self, clock):
        self.clock = clock
        
    # TODO: Update this to use python's built in logging, and to make
    # the output more readable.
    def log(self, *args):
        print args

    # TODO: Update this to use python's built in logging, and to make
    # the output more readable.
    def log_exception(self, title, err, trace, *args):
        self.log(title, *args)
        traceback.print_exception(type(err), err, trace)

    def time(self, name):
        return RunTime(name, self.clock, self.log_run_time)

    def log_run_time(self, rt):
        self.log(rt.name, "in {0:.2f} secs".format(rt.elapsed), rt.result)

    def actor_error(self, name, err, trace):
        self.log_exception("actor error", err, trace, name)

    def actor_died(self, name, err, trace):
        self.log_exception("actor died", err, trace, name)

    def actor_finished(self, name):
        self.log("actor finished", name)

    def path_error(self, err):
        self.log("path error", err)

    def ignored_rpaths(self, rpaths):
        for rpath in rpaths:
            self.log("ignored", rpath)

    def ignored_rpath_without_groupid(self, gpath):
        self.log("ignore rpath without groupid", gpath)

    def ignored_gpath_without_root(self, gpath):
        self.log("ignore gpath without root", gpath)

    def not_a_file(self, path):
        self.log("not a file", path)

    def could_not_hash(self, path):
        self.log("could not hash", path)

    def inserted_history(self, entries):
        for entry in entries:
            self.log("inserted", entry)

    def merged(self, action):
        self.log("merged", action)

    @contextmanager
    def hashing(self, path):
        self.log("begin hashing", path)
        yield
        self.log("end hashing", path)

    @contextmanager
    def copying(self, from_path, to_path):
        self.log("begin copy", from_path, to_path)
        yield
        self.log("end copy", from_path, to_path)

    @contextmanager
    def moving(self, from_path, to_path):
        self.log("begin move", from_path, to_path)
        yield
        self.log("end move", from_path, to_path)

    @contextmanager
    def trashing(self, entry):
        details = entry.hash or (entry.size, entry.mtime)
        self.log("begin trashing", entry.path, details)
        yield
        self.log("end trashing", entry.path, details)

    @contextmanager
    def untrashing(self, entry, dest_path):
        details = entry.hash or (entry.size, entry.mtime)
        self.log("begin untrashing", dest_path, entry.path, details)
        yield
        self.log("end untrashing", dest_path, entry.path, details)

class Config:
    """ This is the config for the entire app.  Right now, there is no
    config file.  Just edit this."""

    # The hash algorithm used when hashing files.  A value of "None"
    # will disable hashing.  That is a lot faster when finding new
    # files, but then fast moves, copies, and undeletes are
    # impossible.
    hash_type = hashlib.sha1

    # A special file.  If present in a directory, that directory will
    # become a new "group root".  If they are never present, then this
    # values doesn't matter.
    group_root_marker = ".psync"

    # Where all of the metadata is stored.  This is relative to the
    # root directory.
    db_path = ".psync/psync.db"

    # The "trash", relative to the root directory. Multiple versions
    # (revisions) of the same file can go in there, so you never lose
    # data that was deleted.  If it's None, then the files really do
    # get deleted.
    revisions_path = ".psync/revisions/"

    # Names to filter out while scanning.  If the name of a directory
    # is given, that directory will not be scanned, which is much
    # faster than filtering after scanning.  So, filtering names like
    # "Library" and ".Trash" on OSX are pretty important.
    names_to_ignore = frozenset([
        # don't scan our selves!
        ".psync",

        # Mac OSX things we shouldn't sync, mostly caches and trashes
        "Library", ".Trash", "iPod Photo Cache", ".DS_Store",

        # Unix things we shouldn't sync, mostly caches and trashes
        ".m2", ".ivy2", ".fontconfig", ".thumbnails", "thumbs.db",
        ".abobe", ".dvdcss", ".cache", ".macromedia", ".xsession-errors",
        ".mozilla", ".java", ".gconf", ".gconfd", ".kde", ".nautilus", ".local",
        ".icons", ".themes",

        # These are debatable.
        ".hg", ".git", ".evolution"])

    # Unix-style "globs" to ignore.  These are slower to test, but are
    # memoized, so they are really only slower on the first scan.  For
    # 5 patterns, the initial scan time is doubled.  But, that time is
    # dwarfed by the hash time anyway.
    globs_to_ignore = \
        [# Parallels big files we probably should never sync
         "*.hds", "*.mem",

         # Contents that change a lot, but we wouldn't want to sync
         ".config/google-chrome/*", ".config/google-googletalkplugin/*",

         # emacs temp files, which we probably never care to sync
         "*~", "*~$", "~*.tmp"]

    # Finally, if you want complete control, you can implement your
    # own PathFilter.
    path_filter = PathFilter(globs_to_ignore, names_to_ignore)


class Groupids(Record("root_by_groupid", "groupid_by_root")):
    def __new__(cls, root_by_groupid):
        groupid_by_root = flip_dict(root_by_groupid)
        return cls.new(root_by_groupid, groupid_by_root)

    def to_root(self, groupid):
        return self.root_by_groupid.get(groupid, None)

    def from_root(self, root):
        return self.groupid_by_root.get(root, None)

# python psync.py source dest
if __name__ == "__main__":
    source_root, dest_root = sys.argv[1:]
    # TODO: What else can we use for peerids when there are no peers?
    source_peerid = source_root
    dest_peerid = dest_root

    # TODO: implement reading .psync.
    source_groupids = Groupids({"": source_root})
    dest_groupids = Groupids({"": dest_root})


    conf = Config()

    clock = Clock()
    slog = StatusLog(clock)
    fs = FileSystem(slog)

    source_db_path = os.path.join(source_root, conf.db_path)
    dest_db_path = os.path.join(dest_root, conf.db_path)
    revisions_root = os.path.join(dest_root, conf.revisions_path)

    fs.create_parent_dirs(source_db_path)
    fs.create_parent_dirs(dest_db_path)
    with sqlite3.connect(source_db_path) as source_db, \
         sqlite3.connect(dest_db_path) as dest_db:
        source_history_store = HistoryStore(SqlDb(source_db), slog)
        dest_history_store = HistoryStore(SqlDb(dest_db), slog)
        revisions = RevisionStore(fs, revisions_root)
        merge_log = MergeLog(SqlDb(source_db), clock)

        source_history = scan_and_update_history(
            fs, source_root,
            conf.group_root_marker, conf.path_filter, conf.hash_type,
            source_history_store, source_peerid, source_groupids,
            clock, slog)

        dest_history = scan_and_update_history(
            fs, dest_root,
            conf.group_root_marker, conf.path_filter, conf.hash_type,
            dest_history_store, dest_peerid, dest_groupids,
            clock, slog)

        filtered_source_history = \
            (entry for entry in source_history
             if (dest_groupids.to_root(entry.groupid) is not None and
                 not conf.path_filter.ignore_path(entry.path)))

        def fetch(entry):
            # We just pretend we fetched it.  Once diff_and_merge
            # moves instead of copies, we'll have to copy the file
            # before we return.
            source_root = source_groupids.to_root(entry.groupid)
            source_path = join_paths(source_root, entry.path)
            return source_path

        # TODO: handle errors,
        #   especially unknown groupid, created! and changed! errors
        diff_and_merge(filtered_source_history, dest_history, dest_groupids,
                       fetch, revisions, fs, dest_history_store, dest_peerid,
                       clock, merge_log, slog)

        # for merge_action in sorted(merge_log.read_actions(dest_peerid)):
        #   print merge_action




    
