# Copyright 2006 Uberan - All Rights Reserved

from history import HistoryEntry, group_history_by_gpath
from FileSystem import DELETED_MTIME, DELETED_SIZE, mtimes_eq, RootedPath
from fs import FileStat, join_paths
from util import Record, Enum, partition, type_constructors

FileDiffType = Enum("created", "changed", "deleted")

@type_constructors(FileDiffType)
class FileDiff(Record("type", "gpath", "rpath", "size", "mtime", "hash")):
    @property
    def was_deleted(entry):
        return entry.mtime == DELETED_MTIME

# groupids must have .to_root and .from_root
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

    with slog.time("reread history") as rt:
        history_entries = history_store.read_entries(peerid)
        history_by_gpath = group_history_by_gpath(history_entries)
        total_size = sum(history.latest.size for history in
                         history_by_gpath.itervalues())
        rt.set_result({"path count": len(history_by_gpath),
                       "total size": total_size})

    return history_entries

# yields FileDiff
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
                    if latest.size != size or not mtimes_eq(latest.mtime, mtime):
                        FileDiff.changed(gpath, rpath, size, mtime, None)
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
                    yield FileDiff.deleted(missing_gpath, RootedPath(root, path),
                                           DELETED_SIZE, DELETED_MTIME, "")

# yields new HistoryEntry if hash is successfull.
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

def new_history_entries_from_file_diffs(fdiffs, peerid, clock):
    utime = int(clock.unix())
    author_peerid = peerid
    author_utime = utime
    for fdiff in fdiffs:
        groupid, path = fdiff.gpath
        yield HistoryEntry(utime, peerid, groupid, path,
                           fdiff.size, fdiff.mtime, fdiff.hash.encode("hex"),
                           author_peerid, author_utime, str(fdiff.type))
