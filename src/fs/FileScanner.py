# Copyright 2006 Uberan - All Rights Reserved

from fs import FileHistoryEntry, join_paths, mtimes_eq
from util import Record, groupby, partition

DELETED_SIZE = 0
DELETED_MTIME = 0

class FileScanner(Record("fs", "clock", "run_timer")):
    def scan_and_update_history(self, fs_root, path_filter, hash_type, history_store):
        scan_and_update_history(
            self.fs, fs_root,
            path_filter.names_to_ignore, path_filter, hash_type,
            history_store, self.clock, self.run_timer)

def scan_and_update_history(fs, fs_root, names_to_ignore, path_filter, hash_type,
                            history_store, clock, run_timer):
    with run_timer("read history"):
        history_entries = history_store.read_entries()
        print ("history entries", len(history_entries))

    with run_timer("scan files"):
        file_stats = list(fs.list_stats(fs_root, names_to_ignore))
        print ("file stats", len(file_stats))

    with run_timer("diff file stats"):
        changed_stats = list(
            diff_file_stats(file_stats, history_entries, path_filter, run_timer))
        print ("changed stats", len(changed_stats))

    with run_timer("hash files"):
        new_history_entries = list(
            hash_file_stats(fs, fs_root, changed_stats, hash_type, clock))
        print ("new history entries", len(new_history_entries))

    with run_timer("rescan files"):
        rescan_stats = list(fs.stats(fs_root, (path for path, size, mtime in changed_stats)))
        print ("rescanned file stats", len(rescan_stats))

    with run_timer("group rescanned stats"):
        rescan_stats_by_path = group_stats_by_path(rescan_stats)

    with run_timer("check change stability"):
        def is_stable(entry):
            (rescan_size, rescan_mtime) = \
                rescan_stats_by_path.get(entry.path, (DELETED_SIZE, DELETED_MTIME))
            #print ("size vs", entry.size, rescan_size)
            #print ("mtime vs", entry.mtime, rescan_mtime)
            return entry.size == rescan_size and entry.mtime == rescan_mtime

        stable_entries, unstable_entries = partition(new_history_entries, is_stable)
        print ("stable entries", len(stable_entries), len(unstable_entries))

    for entry in unstable_entries:
        #*** better logging
        print ("unstable", entry.path)

    # *** remove
    old_paths = frozenset(entry.path for entry in history_entries)
    for entry in stable_entries:
        if entry.mtime == DELETED_MTIME:
            print ("deleted", entry)
        elif entry.path not in old_paths:
            print ("added", entry)
        else:
            print ("changed", entry)

    with run_timer("insert new history entries"):
        history_store.add_entries(stable_entries)

    with run_timer("reread history"):
        history_entries = history_store.read_entries()
        history_by_path = groupby(history_entries, FileHistoryEntry.get_path)
        total_size = sum(max(history).size for history in history_by_path.itervalues())
        print ("new history", len(history_entries), len(history_by_path), total_size)

# yields (path, size, mtime) if the path is new
#        (path, size, mtime) if the size or mtime have changed
#        (path, DELETED_SIZE, DELETED_MTIME) if the path is missing
def diff_file_stats(file_stats, history_entries, path_filter, run_timer):
    with run_timer("group history by path"):
        history_by_path = groupby(history_entries, FileHistoryEntry.get_path)

    with run_timer("compare to latest history"):
        for path, size, mtime in file_stats:
            history = history_by_path.get(path)
            last = None if history is None else max(history)
            if last is not None and last.size == size and mtimes_eq(last.mtime, mtime):
                # unchanged
                pass  
            elif path_filter.ignore_path(path):
                # ignored  
                pass
                #*** improve logging here
                # print ("ignoring", path)
                # logging.warning("ignoring {0}".format(path))
            else:
                # change or created (created if last == None)
                yield path, size, mtime  

    with run_timer("find missing paths"):
        history_paths = frozenset(history_by_path.iterkeys())
        listed_paths = frozenset(path for path, size, mtime in file_stats)
        missing_paths = history_paths - listed_paths
        for path in missing_paths:
            if path_filter.ignore_path(path):
                # ignored
                pass
            else:
                # deleted
                yield path, DELETED_SIZE, DELETED_MTIME

# yields new FileHistoryEntry
def hash_file_stats(fs, fs_root, file_stats, hash_type, clock):
    utime = int(clock.unix())
    for path, size, mtime in file_stats:
        full_path = join_paths(fs_root, path)

        if fs.isfile(full_path):
            try:
                hash = fs.hash(full_path, hash_type)
                yield FileHistoryEntry(path, utime, size, mtime, hash.encode("hex"))
            except IOError:
                pass
                # *** better logging
                # print ("ignoring because could not hash", path)
        else:
            pass
            # *** better logging
            # print ("ignoring because it's not a file", path)

def group_stats_by_path(file_stats):
    return dict((path, (size, mtime)) for path, size, mtime in file_stats)

