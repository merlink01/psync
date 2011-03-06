#TODO:
  # StatusLog
  #   .time(name) -> rt
  #   rt.set_result(val)
  #   .not_a_file(full_path)
  #   .hashing(path)
  #   .could_not_hash(full_path, err)
  #   .copying
  #   .moving
  #   .untrashing(source_entry, dest_path)
  # HistoryStore(path, slog)
  #   calls create_parent_dirs at some point
  #   read_history
  # PathFilter
  #   put .psync in .ignore_path; it's all we use
  # RevisionsStore
  #  copy_out
  # calculate_merge_actions
  # verify_stat
  # copy_file
  # move_file
  # touch_file
  # file_exists
  # trash(dest_path, revisions, entry, slog)
  # get_dest_path(path)
  # first
  # merge
  # source_root in diff_and_merge
  #   verify source before copying?

import hashlib
from collections import namedtuple

DELETED_SIZE = 0
DELETED_MTIME = 0

class HistoryEntry(namedtuple("HistoryEntry",
                              ["utime", "path", "size", "mtime", "hash"])):
    @property
    def deleted(self):
        return self.mtime == DELETED_MTIME

class History(namedtuple("History", ["entries", "latest"])):
    def __new__(cls, entries):
        latest = max(entries)
        return cls._make([entries, latest])

    def __iter__(self):
        return iter(self, entries)

CREATED = "created"
CHANGED = "changed"
DELETED = "deleted"

class FileDiff(namedtuple("FileDiff",
                          ["type", "path", "size", "mtime", "hash"])):
    @property
    def deleted(self):
        return self.mtime == DELETED_MTIME

# returns the updated history
def scan_and_update_history(root, path_filter, hash_type, history_store, slog):
    with slog.time("read history") as rt:
        history = history_store.read_history()
        rt.set_result({"history entries": len(history)})

    with slog.time("scan files") as rt:
        file_stats = list_stats(root, path_filter)
        rt.set_result({"file stats": len(file_stats)})

    with slog.time("diff file stats") as rt:
        fdiffs = diff_file_stats(file_stats, history, slog)
        rt.set_result({"file diffs": len(fdiffs)})

    with slog.time("hash files") as rt:
        hashed_fdiffs = list(hash_file_diffs(root, fdiffs, hash_type, slog))
        rt.set_result({"hashed file diffs": len(hashed_fdiffs)})

    with slog.time("rescan files") as rt:
        rescan_stats = stats(root, path_filter, 
            (fdiff.path for fdiff in hashed_fdiffs))
        rt.set_result({"rescanned file stats": len(rescan_stats)})

    with slog.time("check change stability") as rt:
        rescan_stats_by_path = dict((path, (size, mtime))
                                    for path, size, mtime in file_stats)

        def is_stable(fdiff):
            (rescan_size, rescan_mtime) = rescan_stats_by_path.get(
                fdiff.path, (DELETED_SIZE, DELETED_MTIME))
            return fdiff.size == rescan_size and \
                   mtimes_eq(fdiff.mtime, rescan_mtime)

        stable_fdiffs, unstable_fdiffs = partition(hashed_fdiffs, is_stable)
        rt.set_result({"stable file diffs": len(stable_fdiffs),
                       "unstable file diffs": len(unstable_fdiffs)})

    with slog.time("insert new history entries"):
        utime = int(time.time())
        new_entries = [HistoryEntry(utime, fdiff.path, fdiff.size, fdiff.mtime,
                                    fdiff.hash.encode("hex"))
                       for fdiff in stable_fdiffs]
        if new_entries:
            history_store.add_entries(new_entries)

    return history_store.read_history()

# yields FileDiff
# This is the heart of diffing:
#   If there is a file with no history: created
#   If there is a (non-deleted) history with no file: deleted
#   If the file and history differ: changed
# The trickiest one is to find the missing files, since it requires a
# complete list of files.  A partial list wouldn't work correctly.
def diff_file_stats(file_stats, history, slog):
    with slog.time("group history by path") as rt:
        history_by_path = group_history_by_path(history)
        rt.set_result({"path count": len(history_by_path)})

    with slog.time("compare to latest history"):
        for (path, size, mtime) in file_stats:
            history = history_by_path.pop(path, None)
            latest = history.latest if history is not None else None
            if latest is None:
                yield FileDiff(CREATED, path, size, mtime, None)
            elif latest.size != size \
                     or not mtimes_eq(latest.mtime, mtime):
                yield FileDiff(CHANGED, path, size, mtime, None)
            else:
                pass # unchanged

    with slog.time("find missing paths"):
        for missing_path, missing_history in history_by_path.iteritems():
            if not missing_history.latest.deleted:
                yield FileDiff(DELETED, missing_path,
                               DELETED_SIZE, DELETED_MTIME)

# yields a file_stat_diff with the hash set if hash is successful.
# If not successful, we log the error and simply don't yield the diff.
def hash_file_diffs(root, fdiffs, hash_type, slog):
    for fdiff in fdiffs:
        if fdiff.deleted:
            yield fdiff
        else:
            full_path = os.path.join(root, diffs)
            if not fs.isfile(full_path)
                slog.not_a_file(full_path)
            try:
                # TODO: Put in nice inner-file hashing updates.
                with slog.hashing(full_path):
                    hash = hash_file(full_path, hash_type)

                yield fdiff._replace(hash=hash)
            except IOError as err:
                slog.could_not_hash(full_path, err)

def group_history_by_gpath(entries):
    return groupby(entries, lambda entry: entry.path, into=History)

def diff_and_merge(source_history, dest_history, dest_root,
                   revisions, dest_history_store, slog):
    touches, copies, moves, deletes, undeletes, updates, uphists, conflicts = \
             calculate_merge_actions(source_history, dest_history, revisions)

    # We must do copies and moves first, in case we change the source.
    for action in copies:
        source_latest = first(action.details)
        source_path = verify_stat(source_latest.path, source_latest)
        dest_path = verify_stat(action.path, action.older)
        with slog.copying(source_path, dest_path):
            copy_file(source_path, dest_path, mtime = action.newer.mtime)
        merge(action, history_store, slog)
        
    for action in moves:
        source_latest = action.details
        source_path = verify_stat(source_latest.path, source_latest)
        dest_path = verify_stat(action.path, action.older)
        with slog.moving(source_path, dest_path):
            move_file(source_path, dest_path, mtime = action.newer.mtime)
        merge(action, history_store, slog)

    for action in uphists:
        merge(action, history_store, slog)

    for action in touches:
        dest_path = verify_stat(action.path, action.older)
        touch_file(dest_path, action.newer.mtime)
        merge(action, history_store, slog)

    for action in deletes:
        dest_path = get_dest_path(action.path)
        if file_exists(dest_path):
            dest_path = verify_stat(action.path, action.older)
            trash(dest_path, revisions, action.older, slog)
        merge(action, history_store, slog)

    for action in undeletes:
        rev_entry = action.details
        dest_path = verify_stat(action.gpath, action.older)
        trash(dest_path, revisions, action.older, slog)
        with slog.untrashing(rev_entry, dest_path):
            revisions.copy_out(rev_entry, dest_path)
        merge(action, history_store, slog)

    for action in conflicts:
        older, newer = action.older, action.newer
        # When mtimes are equal, use utime, size, and hash as tie-breakers.
        if (newer.mtime, newer.utime, newer.size, newer.hash) \
               > (older.mtime, older.utime, older.size, older.hash):
            updates.append(action)

    for action in updates:
        # TODO: Allow fetch to be asyncronous somehow.
        source_path = os.path.join(source_root, action.path)
        dest_path = verify_stat(action.gpath, action.older)
        trash(dest_path, revisions, action.older, slog)
        with slog.copying(source_path, dest_path):
            copy_file(source_path, dest_path, mtime = action.newer.mtime)
        merge(action, history_store, slog)


## File System utils ##

def list_stats(root, path_filter):
    return stats(root, list_paths(root, path_filter))

STAT_SIZE_INDEX  = 6
STAT_MTIME_INDEX = 8

def stats(root, paths):
    stat = os.stat
    for path in paths:
        try:
            stats = stat(path)
            size = stats[STAT_SIZE_INDEX]
            mtime = stats[STAT_MTIME_INDEX]
            yield FileStat(path, size, mtime)
        except OSError:
            pass  # Probably a link

# This needs to be as fast as possible.
def list_paths(root, path_filter):
    listdir = os.listdir
    join = os.path.join
    isdir = os.path.isdir
    islink = os.path.islink
    # TODO: can we use path_filter.ignore_path() on all names and
    # still be as fast?

    def walk(parent):
        for child_name in listdir(parent):
            child = join(parent, child_name)
            rel_child = child[len(parent)+1:]
            if not path_filter.ignore_path(rel_child):
                if isdir(child):
                    if not islink(child):
                        for child_child in walk(child):
                            yield child_child
                else:
                    yield rel_child

    return walk(root)


# Windows shaves off a bit of mtime info.
# TODO: Only do this sillyness on Windows.
def mtimes_eq(mtime1, mtime2):
    return (mtime1 >> 1) == (mtime2 >> 1)

def hash_file(path, hash_type, chunk_size = 100000):
    if hash_type == None:
        return ""

    hasher = hash_type()
    for chunk_data in iter_file_chunks(path, chunk_size):
        hasher.update(chunk_data)
    return hasher.digest()

READ_MODE = "rb"

def iter_file_chunks(path, chunk_size):
    with open(path, READ_MODE) as file:
        chunk = file.read(chunk_size)
        while chunk:
            yield chunk
            chunk = file.read(chunk_size)

## General utils ##

def partition(vals, predicate):
    trues, falses = [], []
    for val in vals:
        (trues if predicate(val) else falses).append(val)
    return trues, falses
                           
def groupby(vals, key = None, into = None):
    group_by_key = {}
    for val in vals:
        group = group_by_key.setdefault(key(val), [])
        group.append(val)

    if into is not None:
        for key in group_by_key:
            group_by_key[key] = into(group_by_key[key])

    return group_by_key


if __name__ == "__main__":
    # python psync_simple.py source dest
    import sys
    source_root, dest_root = sys.argv[1:]

    hash_type = hashlib.sha1
    rel_history_path = ".psync/history.db"
    rel_revisions_path = ".psync/revisions/"
    names_to_ignore = frozenset([".pysnc"])  # .git, .hg?
    globs_to_ignore = ["*~", "*~$", "~*.tmp", "*.DS_Store"]

    slog = StatusLog()
    source_history_store = HistoryStore(
        os.path.join(source_root, rel_history_path), slog)
    dest_history_store = HistoryStore(
        os.path.join(dest_root, rel_history_path), slog)
    revisions = RevisionStore(
        os.path.join(dest_root, rel_revisions_path))
    path_filter = PathFilter(globs_to_ignore, names_to_ignore)
    
    source_history = scan_and_update_history(
        source_root, path_filter, hash_type, source_history_store, slog)
    dest_history = scan_and_update_history(
        dest_root, path_filter, hash_type, dest_history_store, slog)
    diff_and_merge(source_history, dest_history, dest_root,
                   revisions, dest_history_store, slog)
        
    
