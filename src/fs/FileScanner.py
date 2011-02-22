# Copyright 2006 Uberan - All Rights Reserved

from fs import (FileHistoryEntry, group_history_by_path,
                join_paths, mtimes_eq, latest_history_entry,
                DELETED_SIZE, DELETED_MTIME)
from util import Record, partition

class FileScanner(Record("fs", "clock", "run_timer")):
    def scan_and_update_history(self, fs_root, path_filter, hash_type,
                                history_store, peerid):
        scan_and_update_history(
            self.fs, fs_root,
            path_filter.names_to_ignore, path_filter, hash_type,
            history_store, peerid, self.clock, self.run_timer)

def scan_and_update_history(fs, fs_root, names_to_ignore, path_filter, hash_type,
                            history_store, peerid, clock, run_timer):
    with run_timer("read history") as rt:
        history_entries = history_store.read_entries()
        rt.set_result({"history entries": len(history_entries)})

    with run_timer("scan files") as rt:
        file_stats = list(fs.list_stats(fs_root, names_to_ignore))
        rt.set_result({"file stats": len(file_stats)})

    with run_timer("diff file stats") as rt:
        changed_stats = list(
            diff_file_stats(file_stats, history_entries, path_filter, run_timer))
        rt.set_result({"changed stats": len(changed_stats)})

    with run_timer("hash files") as rt:
        # **** Add add|changed|deleted to new entries
        # if entry.deleted:
        #     print ("deleted", entry)
        # elif entry.path not in history_by_path:
        #     print ("added", entry)
        # else:
        #     print ("changed", entry)
        new_history_entries = list(
            hash_file_stats(fs, fs_root, changed_stats, hash_type,
                            peerid, clock))
        rt.set_result({"new history entries": len(new_history_entries)})

    with run_timer("rescan files") as rt:
        rescan_stats = list(fs.stats(fs_root, (path for path, size, mtime in changed_stats)))
        rt.set_result({"rescanned file stats": len(rescan_stats)})

    with run_timer("group rescanned stats"):
        rescan_stats_by_path = group_stats_by_path(rescan_stats)

    with run_timer("check change stability") as rt:
        def is_stable(entry):
            (rescan_size, rescan_mtime) = \
                rescan_stats_by_path.get(entry.path,
                                         (DELETED_SIZE, DELETED_MTIME))
            return entry.size == rescan_size and entry.mtime == rescan_mtime

        stable_entries, unstable_entries = \
                        partition(new_history_entries, is_stable)
        rt.set_result({"stable entries": len(stable_entries),
                       "unstable entries": len(unstable_entries)})

    with run_timer("insert new history entries"):
        history_store.add_entries(stable_entries)

    with run_timer("reread history") as rt:
        history_entries = history_store.read_entries()
        history_by_path = group_history_by_path(history_entries)
        total_size = sum(latest_history_entry(history).size for history in
                         history_by_path.itervalues())
        rt.set_result({"path count": len(history_by_path),
                       "total size": total_size})

# yields (path, size, mtime) if the path is new
#        (path, size, mtime) if the size or mtime have changed
#        (path, DELETED_SIZE, DELETED_MTIME) if the path is missing
def diff_file_stats(file_stats, history_entries, path_filter, run_timer):
    with run_timer("group history by path") as rt:
        history_by_path = group_history_by_path(history_entries)
        rt.set_result({"path count": len(history_by_path)})
        

    with run_timer("compare to latest history"):
        #**** use enum to avoid filter in two places
        for path, size, mtime in file_stats:
            history = history_by_path.get(path)
            latest = None if history is None else latest_history_entry(history)
            if latest is not None \
                   and latest.size == size \
                   and mtimes_eq(latest.mtime, mtime):
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
def hash_file_stats(fs, fs_root, file_stats, hash_type, peerid, clock):
    utime = int(clock.unix())
    for path, size, mtime in file_stats:
        full_path = join_paths(fs_root, path)

        if fs.isfile(full_path):
            try:
                hash = fs.hash(full_path, hash_type)
                author_peerid = peerid
                author_utime = utime
                yield FileHistoryEntry(
                    utime, peerid, path, size, mtime, hash.encode("hex"),
                    author_peerid, author_utime)
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


