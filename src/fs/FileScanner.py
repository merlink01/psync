# Copyright 2006 Uberan - All Rights Reserved

from fs import (FileHistoryEntry, group_history_by_path,
                join_paths, mtimes_eq, latest_history_entry,
                DELETED_SIZE, DELETED_MTIME)
from util import Record, Enum, partition

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
        diff_stats = diff_file_stats(file_stats, history_entries, slog)
        changed_diff_stats, unchanged_diff_stats = partition(diff_stats,
            lambda (diff, path, size, mtime): diff != FileDiff.unchanged)
        ignored_diff_stats, changed_diff_stats = partition(changed_diff_stats,
            lambda (diff, path, size, mtime): path_filter.ignore_path(path))
        slog.ignored_paths(path for (diff, path, size, mtime)
                           in ignored_diff_stats)
        rt.set_result({"changed diff stats": len(changed_diff_stats)})

    with slog.time("hash files") as rt:
        new_history_entries = list(
            hash_file_diffs(fs, fs_root, changed_diff_stats, hash_type,
                            peerid, clock, slog))
        rt.set_result({"new history entries": len(new_history_entries)})

    with slog.time("rescan files") as rt:
        rescan_stats = list(fs.stats(fs_root, \
            (path for diff, path, size, mtime in changed_diff_stats)))
        rt.set_result({"rescanned file stats": len(rescan_stats)})

    with slog.time("group rescanned stats"):
        rescan_stats_by_path = group_stats_by_path(rescan_stats)

    with slog.time("check change stability") as rt:
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

FileDiff = Enum("unchanged", "created", "changed", "deleted")

# yields (FileDiff, path, size, mtime)
def diff_file_stats(file_stats, history_entries, slog):
    with slog.time("group history by path") as rt:
        history_by_path = group_history_by_path(history_entries)
        rt.set_result({"path count": len(history_by_path)})
        
    with slog.time("compare to latest history"):
        for path, size, mtime in file_stats:
            history = history_by_path.get(path)
            latest = None if history is None else latest_history_entry(history)
            if latest is None:
                yield (FileDiff.created, path, size, mtime)
            elif latest.size == size and mtimes_eq(latest.mtime, mtime):
                yield (FileDiff.unchanged, path, size, mtime)
            else:
                yield (FileDiff.changed, path, size, mtime)

    with slog.time("find missing paths"):
        missing_paths = (frozenset(history_by_path.iterkeys())
                         - frozenset(path for path, size, mtime in file_stats))
        for path in missing_paths:
            # TODO: this could probably be more efficient
            latest = latest_history_entry(history_by_path.get(path))
            if not latest.deleted:
                yield (FileDiff.deleted, path, DELETED_SIZE, DELETED_MTIME)

# yields new FileHistoryEntry
# TODO: Rename to "get FileHistoryEntries" or something like that.
def hash_file_diffs(fs, fs_root, diff_stats, hash_type, peerid, clock, slog):
    utime = int(clock.unix())
    author_peerid = peerid
    author_utime = utime

    for diff, path, size, mtime in diff_stats:
        if mtime == DELETED_MTIME:
            hash = ""
        else:
            hash = hash_path(fs, fs_root, path, hash_type, slog)
            
        if hash is not None:
            yield FileHistoryEntry(
                utime, peerid, path, size, mtime, hash.encode("hex"),
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

def group_stats_by_path(file_stats):
    return dict((path, (size, mtime)) for path, size, mtime in file_stats)


