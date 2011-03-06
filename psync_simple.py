## TODO:
# Handle unicode paths, especially in Windows.
# StatusLog
#   .time(name) -> rt
#   rt.set_result(val)
#   .not_a_file(full_path)
#   .hashing(path)
#   .could_not_hash(full_path, err)
#   .copying
#   .moving
#   .trashing
#   .untrashing(source_entry, dest_path)
# HistoryStore(path, slog)
#   calls create_parent_dirs at some point
#   read_history
# PathFilter
#   put .psync in .ignore_path; it's all we use
# RevisionsStore
#  copy_out
#  move_in

from collections import namedtuple
import hashlib
import os
import shutil

DELETED_SIZE = 0
DELETED_MTIME = 0

class HistoryEntry(namedtuple("HistoryEntry",
                              ["utime", "path", "size", "mtime", "hash",
                               "source_utime"])):
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

IN_SYNC = "in sync"
NEWER = "newer"
OLDER = "older"
IN_CONFLICT = "in conflict"
HISTORY_CONFLICT = "history conflict"

class HistoryDiff(namedtuple("HistoryDiff",
                             ["type", "path", "latest1", "latest2"])):
    def __new__(cls, type, latest1, latest2, details = None):
        path = latest1.path if latest2 is None else latest2.path
        return cls._make([type, path, older, latest2])

TOUCH = "touch"
COPY = "copy"
MOVE = "move"
DELETE = "delete"
UNDELETE = "undelete"
UPDATE = "update"
UPDATE_HISTORY = "update history"
RESOLVE_CONFLICT = "resolve conflict"

class MergeAction(namedtuple("MergeAction",
                             ["type", "path", "older", "newer", "details"])):
    def __new__(cls, type, older, newer, details = None):
        path = older.path if newer is None else newer.path
        return cls._make([type, path, older, newer, details])

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
        source_utime = utime
        new_entries = [HistoryEntry(utime, fdiff.path, fdiff.size, fdiff.mtime,
                                    fdiff.hash.encode("hex"), source_utime)
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

def diff_and_merge(source_root, source_history,
                   dest_root, dest_history,
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
        add_new_entry(action, history_store, slog)
        
    for action in moves:
        source_latest = action.details
        source_path = verify_stat(source_latest.path, source_latest)
        dest_path = verify_stat(action.path, action.older)
        with slog.moving(source_path, dest_path):
            move_file(source_path, dest_path, mtime = action.newer.mtime)
        add_new_entry(action, history_store, slog)

    for action in uphists:
        add_new_entry(action, history_store, slog)

    for action in touches:
        dest_path = verify_stat(action.path, action.older)
        touch_file(dest_path, action.newer.mtime)
        add_new_entry(action, history_store, slog)

    for action in deletes:
        dest_path = os.path.join(dest_root, action.path)
        if os.path.exists(dest_path):
            dest_path = verify_stat(action.path, action.older)
            trash_file(dest_path, revisions, action.older, slog)
        add_new_entry(action, history_store, slog)

    for action in undeletes:
        rev_entry = action.details
        dest_path = verify_stat(action.gpath, action.older)
        trash_file(dest_path, revisions, action.older, slog)
        with slog.untrashing(rev_entry, dest_path):
            revisions.copy_out(rev_entry, dest_path)
        add_new_entry(action, history_store, slog)

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
        trash_file(dest_path, revisions, action.older, slog)
        with slog.copying(source_path, dest_path):
            copy_file(source_path, dest_path, mtime = action.newer.mtime)
        add_new_entry(action, history_store, slog)

def verify_stat(dest_root, path, latest):
    full_path = os.path.join(dest_root, path)

    # TODO: Handle these errors better.
    if latest is None or latest.deleted:
        if fs.exists(full_path):
            raise Exception("file created!", full_path)
    else:
        if not file_stat_eq(full_path, latest.size, latest.mtime):
            raise Exception("file changed!", full_path)

    return full_path

def file_stat_eq(path, size, mtime):
    try:
        stats = os.stat(path)
        current_size = stats[STAT_SIZE_INDEX]
        current_mtime = stats[STAT_MTIME_INDEX]
        return (current_size == size and mtimes_eq(current_mtime, mtime))
    except OSError:
        return False

def trash_file(source_path, revisions, dest_entry, slog):
    if os.path.exists(source_path):
        with slog.trashing(dest_entry):
            revisions.move_in(source_path, dest_entry)
            remove_empty_parent_dirs(source_path)

def add_new_entry(action, history_store, slog):
    new_entry = action.newer._replace(utime=int(time.time())
    history_store.add_entries([new_entry])
    slog.merged(action)

# yields MergeAction
# First, found the source entries which are newer or in conflict.  Then:
#  if newer is deleted, delete
#  if newer is the same contents, touch
#  if newer hash is the copy of something deleted, move
#  if newer hash already exists locally, copy
#  if newer hash is in the trash, undelete
#  if history conflict: resolve by updating local history
#  if conflict: pass along as a conflict
#  otherwise: update
def calculate_merge_actions(source_history, dest_history, revisions):
    actions = iter_merge_actions_without_moves(
        source_history, dest_history, revisions)
    action_by_type = groupby(actions, lambda action: action.type)
    touches, copies, moves, deletes, undeletes, updates, uphists, conflicts = \
             (action_by_type.get(type, []) for type in
              [TOUCH, COPY, MOVE, DELETE, UNDELETE, UPDATE,
               UPDATE_HISTORY, RESOLVE_CONFLICT])

    moves = []
    unmoved_copies = []

    # If a copy also has a matching delete, make it as "move".
    deletes_by_hash = groupby(deletes, \
            lambda delete: delete.older.hash if delete.older else None)
    for action in copies:
        deletes_of_hash = deletes_by_hash.get(action.newer.hash, [])
        if action.newer.hash and deletes_of_hash:
            # Pop so we only match a given delete once.  But we
            # leave the deleted in the actions so that it's put
            # in the history and merge data, but we don't put it
            # in the revisions.
            delete = deletes_of_hash.pop()
            moves.append(action._update(type = MOVE, details = delete.older))
        else:
            unmoved_copies.append(action)
    copies = unmoved_copies

    return (touches, copies, moves, deletes, undeletes,
            updates, uphists, conflicts)

def iter_merge_actions_without_moves(source_history, dest_history, revisions):
    source_history_by_path = group_history_by_path(source_history)
    dest_history_by_path = group_history_by_path(dest_history)
    dest_latests_by_hash = \
            latests_by_hash_from_history_by_path(dest_history_by_path)
    dest_entries_by_hash = \
            entries_by_hash_from_history_by_path(dest_history_by_path)

    for (diff, path, newer, older) in \
            diff_histories(source_history_by_path, dest_history_by_path):
        if diff == NEWER:
            if newer.deleted:
                yield MergeAction(DELETE, older, newer)
            # Same content, just mtime differs
            elif (older != None and entries_contents_match(older, newer)):
                yield MergeAction(TOUCH, older, newer)
            # Availalbe locally, just copy.
            elif newer.hash and newer.hash in dest_latests_by_hash:
                local_sources = dest_latests_by_hash[newer.hash]
                yield MergeAction(COPY, older, newer, local_sources)
            # Not deleted, different content, and not available locally
            else:
                revision_entry = first(rev_entry for rev_entry in
                                       dest_entries_by_hash.get(newer.hash, [])
                                       if rev_entry in revisions)
                # In the trash
                if revision_entry:
                    yield MergeAction(UNDELETE, older, newer, revision_entry)
                else:
                    yield MergeAction(UPDATE, older, newer)
        elif diff == HistoryDiffType.history_conflict:
            if older.deleted:
                yield MergeAction(UPDATE_HISTORY, older, newer)
            else:
                yield MergeAction(TOUCH, older, newer)
        elif diff == HistoryDiffType.conflict:
            yield MergeAction(RESOLVE_CONFLICT, older, newer)

# yields all HistoryDiffs
def diff_histories(history_by_path1, history_by_path2):
    for path1, history1 in history_by_path1.iteritems():
        history2 = history_by_path2.get(path1)
        latest1 = history1.latest
        latest2 = None if history2 is None else history2.latest
        if latest2 is None:
            yield HistoryDiff(NEWER, latest1, latest2)
        elif entries_match(latest1, latest2):
            yield HistoryDiff(IN_SYNC, latest1, latest2)
        elif entries_contents_match(latest1, latest2):
            yield HistoryDiff(HISTORY_CONFLICT, latest1, latest2)
        elif has_matching_entry(history1, latest2):
            yield HistoryDiff(NEWER, latest1, latest2)
        elif has_matching_entry(history2, latest1):
            yield HistoryDiff(OLDER, latest1, latest2)
        else:
            yield HistoryDiff(IN_CONFLICT, latest1, latest2)

    for path2, history2 in history_by_path2.iteritems():
        history1 = history_by_path1.get(path2)
        if history1 is None:
            yield HistoryDiff(OLDER, history2.latest)

def entries_contents_match(entry1, entry2):
    return (entry1.size == entry2.size and
            entry1.hash == entry2.hash)

def entries_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.mtime == entry2.mtime and
            entry1.hash  == entry2.hash and
            entry1.source_utime == entry2.source_utime)

def has_matching_entry(history, entry):
    return any(entries_match(entry, entry2) for entry2 in history)

def latests_by_hash_from_history_by_path(history_by_path):
    latests_by_hash = {}
    for history in history_by_path.itervalues():
        latest = history.latest
        if latest.hash:
            setdefault(latests_by_hash, latest.hash, set).add(latest)
    return latests_by_hash

def entries_by_hash_from_history_by_path(history_by_path):
    entries_by_hash = {}
    for history in history_by_path.itervalues():
        for entry in history:
            if entry.hash:
                setdefault(entries_by_hash, entry.hash, set).add(entry)
    return entries_by_hash

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

def copy_file(from_path, to_path, mtime = None):
    create_parent_dirs(to_path)
    shutil.copyfile(from_path, to_path)
    if mtime is not None:
        touch_file(to_path, mtime)

def move_file(from_path, to_path, mtime = None):
    create_parent_dirs(to_path)
    os.rename(from_path, to_path)
    if mtime is not None:
        touch_file(to_path, mtime)

def touch_file(path, mtime):
    os.utime(path, (mtime, time))

def remove_empty_parent_dirs(fs, path):
    try:
        os.removedirs(path)
    except OSError:
        pass  # Not empty

def create_parent_dirs(path):
    fs.create_dir(os.path.dirname(path))

def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    

## General utils ##

def first(itr, default = None):
    for val in itr:
        return val
    return default

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
    diff_and_merge(source_root, source_history, dest_root, dest_history,
                   revisions, dest_history_store, slog)
