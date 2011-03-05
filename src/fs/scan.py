# Copyright (c) 2012, Peter Thatcher
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


# The purpose of this file is to do a scan of the file system, compare
# it to the last scan, figure out what files were changed, and then
# update that history.  If a file has changed, it will also hash the
# file.  We also try to do a few things intelligently, like filtering
# in the fastest possible way and making sure the files are "stable".

from history import HistoryEntry, group_history_by_gpath
from FileSystem import DELETED_MTIME, DELETED_SIZE, mtimes_eq, RootedPath
from fs import FileStat, join_paths
from util import Record, Enum, partition, type_constructors

FileDiffType = Enum("created", "changed", "deleted")

# gpath is a GroupedPath (as in history)
# rpath is a RootedPath (as in new scan)
@type_constructors(FileDiffType)
class FileDiff(Record("type", "gpath", "rpath", "size", "mtime", "hash")):
    @property
    def was_deleted(entry):
        return entry.mtime == DELETED_MTIME

# This is pretty much the whole purpose of this file: scan the file
# system, find what's different, and update the history.  To do so, we
# need to be able to convert from root paths to groupids and back.  To
# do so, we expect an object called "groupids" which takes a .to_root
# and .from_root.
def scan_and_update_history(fs, fs_root, root_mark, path_filter, hash_type,
                            history_store, peerid, groupids, clock, slog):
    with slog.time("read history") as rt:
        history_entries = history_store.read_entries(peerid)
        rt.set_result({"history entries": len(history_entries)})

    with slog.time("scan files") as rt:
        file_stats = list(fs.list_stats(
            fs_root, root_mark, names_to_ignore = path_filter.names_to_ignore))
        rt.set_result({"file stats": len(file_stats)})

    with slog.time("diff file stats") as rt:
        fdiffs = diff_file_stats(file_stats, history_entries, groupids, slog)
        ignored_fdiffs, fdiffs = partition(fdiffs,
            lambda fdiff: path_filter.ignore_path(fdiff.rpath.full))
        slog.ignored_rpaths(fdiff.rpath for fdiff in ignored_fdiffs)
        rt.set_result({"file diffs": len(fdiffs)})

    with slog.time("hash files") as rt:
        hashed_fdiffs = list(hash_file_diffs(fs, fdiffs, hash_type, slog))
        rt.set_result({"hashed file diffs": len(hashed_fdiffs)})

    # We rescan the files to make sure they are stable.  We might
    # decided to do this before hashing if there are lots of big
    # unstable files.  But I think we'll usually be stable.
    with slog.time("rescan files") as rt:
        rescan_stats = list(fs.stats(
            (fdiff.rpath for fdiff in hashed_fdiffs)))
        rt.set_result({"rescanned file stats": len(rescan_stats)})

    with slog.time("check change stability") as rt:
        rescan_stats_by_rpath = dict((rpath, (size, mtime))
                                     for rpath, size, mtime in file_stats)

        def is_stable(fdiff):
            (rescan_size, rescan_mtime) = rescan_stats_by_rpath.get(
                fdiff.rpath, (DELETED_SIZE, DELETED_MTIME))
            return fdiff.size == rescan_size and \
                   mtimes_eq(fdiff.mtime, rescan_mtime)

        stable_fdiffs, unstable_fdiffs = partition(hashed_fdiffs, is_stable)
        rt.set_result({"stable file diffs": len(stable_fdiffs),
                       "unstable file diffs": len(unstable_fdiffs)})

    with slog.time("insert new history entries"):
        new_entries = list(new_history_entries_from_file_diffs(
            stable_fdiffs, peerid, clock))
        if new_entries:
            history_store.add_entries(new_entries)

    # Techincally, we don't have to do this, but it's nice to log this
    # after every scan.
    with slog.time("reread history") as rt:
        history_entries = history_store.read_entries(peerid)
        history_by_gpath = group_history_by_gpath(history_entries)
        total_size = sum(history.latest.size for history in
                         history_by_gpath.itervalues())
        rt.set_result({"path count": len(history_by_gpath),
                       "total size": total_size})

    return history_entries

# yields FileDiff
# This is the heart of diffing:
#   If there is a file with no history: created
#   If there is a (non-deleted) history with no file: deleted
#   If the file and history differ: changed
# The trickiest one is to find the missing files, since it requires a
# complete list of files.  A partial list wouldn't work correctly.
def diff_file_stats(file_stats, history_entries, groupids, slog):
    with slog.time("group history by path") as rt:
        history_by_gpath = group_history_by_gpath(history_entries)
        rt.set_result({"path count": len(history_by_gpath)})

    with slog.time("compare to latest history"):
        for (rpath, size, mtime) in file_stats:
            groupid = groupids.from_root(rpath.root)
            if groupid is None:
                slog.ignored_rpath_without_groupid(rpath)
            else:
                gpath = (groupid, rpath.rel)
                history = history_by_gpath.pop(gpath, None)
                if history is None:
                    yield FileDiff.created(gpath, rpath, size, mtime, None)
                else:
                    latest = history.latest
                    if latest.size != size \
                           or not mtimes_eq(latest.mtime, mtime):
                        yield FileDiff.changed(gpath, rpath, size, mtime, None)
                    else:
                        pass # unchanged

    with slog.time("find missing paths"):
        for missing_gpath, missing_history in history_by_gpath.iteritems():
            if not missing_history.latest.deleted:
                (groupid, path) = missing_gpath
                root = groupids.to_root(groupid)
                if root is None:
                    slog.ignored_gpath_without_root(missing_gpath)
                else:
                    yield FileDiff.deleted(
                        missing_gpath, RootedPath(root, path),
                        DELETED_SIZE, DELETED_MTIME, "")

# yields a file_stat_diff with the hash set if hash is successful.
# If not successful, we log the error and simply don't yield the diff.
def hash_file_diffs(fs, fdiffs, hash_type, slog):
    for fdiff in fdiffs:
        if fdiff.was_deleted:
            yield fdiff
        else:
            full_path = fdiff.rpath.full
            if not fs.isfile(full_path):
                slog.not_a_file(full_path)
            try:
                # TODO: Put in nice inner-file hashing updates.
                with slog.hashing(full_path):
                    hash = fs.hash(full_path, hash_type)

                yield fdiff.set_hash(hash)
            except IOError as err:
                slog.could_not_hash(full_path, err)

# Convert a file diff into a new history entry.  The hash is encoded
# in hex.  We take the current time for the "utime".
def new_history_entries_from_file_diffs(fdiffs, peerid, clock):
    utime = int(clock.unix())
    author_peerid = peerid
    author_utime = utime
    for fdiff in fdiffs:
        groupid, path = fdiff.gpath
        yield HistoryEntry(utime, peerid, groupid, path,
                           fdiff.size, fdiff.mtime, fdiff.hash.encode("hex"),
                           author_peerid, author_utime, str(fdiff.type))
