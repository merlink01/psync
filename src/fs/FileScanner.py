# Copyright 2006 Uberan - All Rights Reserved

from fs import (FileHistoryEntry, group_history_by_path,
                join_paths, mtimes_eq, latest_history_entry,
                DELETED_SIZE, DELETED_MTIME)
from util import Record, Enum, partition

FileDiff = Enum("unchanged", "created", "changed", "deleted")

class FileScanner(Record("fs", "clock", "slog")):
    def scan_and_update_history(self, fs_root, path_filter, hash_type,
                                history_store, peerid):
        scan_and_update_history(
            self.fs, fs_root,
            path_filter.names_to_ignore, path_filter, hash_type,
            history_store, peerid, self.clock, self.slog)

def scan_and_update_history(fs, fs_root,
                            names_to_ignore, path_filter, hash_type,
                            history_store, peerid, clock, slog):
    with slog.time("read history") as rt:
        history_entries = history_store.read_entries(peerid)
        rt.set_result({"history entries": len(history_entries)})

    with slog.time("scan files") as rt:
        file_stats = list(fs.list_stats(fs_root, names_to_ignore))
        rt.set_result({"file stats": len(file_stats)})

    with slog.time("diff file stats") as rt:
        dfstats = diff_file_stats(file_stats, history_entries, slog)
        changed_dfstats, unchanged_dfstats = partition(dfstats,
            lambda (diff, fstat): diff != FileDiff.unchanged)
        ignored_dfstats, changed_dfstats = partition(changed_dfstats,
            lambda (diff, fstat): path_filter.ignore_path(fstat.path))
        slog.ignored_paths(fstat.path for (diff, fstat) in ignored_dfstats)
        rt.set_result({"changed diff stats": len(changed_dfstats)})

    with slog.time("hash files") as rt:
        new_history_entries = list(
            hash_diff_stats(fs, fs_root, changed_dfstats, hash_type,
                            peerid, clock, slog))
        rt.set_result({"new history entries": len(new_history_entries)})

    with slog.time("rescan files") as rt:
        rescan_stats = list(fs.stats(fs_root, \
            (fstat.path for diff, fstat in changed_dfstats)))
        rt.set_result({"rescanned file stats": len(rescan_stats)})

    with slog.time("check change stability") as rt:
        rescan_stats_by_path = dict((path, (size, mtime))
                                    for path, size, mtime in file_stats)

        def is_stable(entry):
            (rescan_size, rescan_mtime) = \
                rescan_stats_by_path.get(entry.path,
                                         (DELETED_SIZE, DELETED_MTIME))
            return entry.size == rescan_size and entry.mtime == rescan_mtime

        stable_entries, unstable_entries = \
            partition(new_history_entries, is_stable)
        rt.set_result({"stable entries": len(stable_entries),
                       "unstable entries": len(unstable_entries)})

    with slog.time("insert new history entries"):
        if stable_entries:
            history_store.add_entries(stable_entries)

    with slog.time("reread history") as rt:
        history_entries = history_store.read_entries(peerid)
        history_by_path = group_history_by_path(history_entries)
        total_size = sum(latest_history_entry(history).size for history in
                         history_by_path.itervalues())
        rt.set_result({"path count": len(history_by_path),
                       "total size": total_size})

# yields (FileDiff, file_stat)
def diff_file_stats(file_stats, history_entries, slog):
    with slog.time("group history by path") as rt:
        history_by_path = group_history_by_path(history_entries)
        rt.set_result({"path count": len(history_by_path)})
        
    with slog.time("compare to latest history"):
        for fstat in file_stats:
            history = history_by_path.get(fstat.path)
            latest = None if history is None else latest_history_entry(history)
            if latest is None:
                yield (FileDiff.created, fstat)
            elif latest.size == fstat.size \
                    and mtimes_eq(latest.mtime, fstat.mtime):
                yield (FileDiff.unchanged, fstat)
            else:
                yield (FileDiff.changed, fstat)

    with slog.time("find missing paths"):
        missing_paths = (frozenset(history_by_path.iterkeys())
                         - frozenset(fstat.path for fstat in file_stats))
        for missing_path in missing_paths:
            # TODO: this could probably be more efficient
            latest = latest_history_entry(history_by_path.get(missing_path))
            if not latest.deleted:
                yield (FileDiff.deleted, FileStat.from_deleted(missing_path))

# yields new FileHistoryEntry
# TODO: Rename to "get FileHistoryEntries" or something like that.
def hash_diff_stats(fs, fs_root, dfstats, hash_type, peerid, clock, slog):
    utime = int(clock.unix())
    author_peerid = peerid
    author_utime = utime

    for diff, fstat in dfstats:
        if fstat.deleted:
            hash = ""
        else:
            hash = hash_path(fs, fs_root, fstat.path, hash_type, slog)
            
        if hash is not None:
            yield FileHistoryEntry(
                utime, peerid, fstat.path,
                fstat.size, fstat.mtime, hash.encode("hex"),
                author_peerid, author_utime, str(diff))

def hash_path(fs, fs_root, path, hash_type, slog):
    full_path = join_paths(fs_root, path)

    if not fs.isfile(full_path):
        slog.not_a_file(full_path)
        return None

    try:
        # TODO: Put in nice inner-file hashing updates.
        with slog.hashing(full_path):
            hash = fs.hash(full_path, hash_type)
        return hash
    except IOError as err:
        slog.could_not_hash(full_path, err)
        return None

