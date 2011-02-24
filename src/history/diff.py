# Copyright 2006 Uberan - All Rights Reserved

from entry import History, HistoryEntry, group_history_by_path
from util import Record, Enum, type_constructors, setdefault

HistoryDiffType = Enum("insync", "newer", "older",
                       "conflict", "history_conflict")

# So you can say HistoryDiff.newer(latest1, latest1)
@type_constructors(HistoryDiffType)
class HistoryDiff(Record("type", "path", "latest1", "latest2")):
    def __new__(cls, type, latest1, latest2):
        path = get_path(latest1, latest2)
        return cls.new(type, path, latest1, latest2)
    
# yield HistoryDiff
def diff_histories(history_by_path1, history_by_path2):
    for path1, history1 in history_by_path1.iteritems():
        history2 = history_by_path2.get(path1)
        latest1 = history1.latest
        latest2 = None if history2 is None else history2.latest
        if latest2 is None:
            yield HistoryDiff.newer(latest1, latest2)
        elif entries_match(latest1, latest2):
            yield HistoryDiff.insync(latest1, latest2)
        elif entries_contents_match(latest1, latest2):
            yield HistoryDiff.history_conflict(latest1, latest2)
        elif has_matching_entry(history1, latest2):
            yield HistoryDiff.newer(latest1, latest2)
        elif has_matching_entry(history2, latest1):
            yield HistoryDiff.older(latest1, latest2)
        else:
            yield HistoryDiff.conflict(latest1, latest2)

    for path2, history2 in history_by_path2.iteritems():
        history1 = history_by_path1.get(path2)
        if history1 is None:
            yield HistoryDiff.older(None, history2.latest)

def entries_contents_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.hash  == entry2.hash)

def entries_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.mtime == entry2.mtime and
            entry1.hash  == entry2.hash and
            entry1.author_peerid == entry2.author_peerid and
            entry1.author_utime == entry2.author_utime)
        
def has_matching_entry(history, entry):
    return any(entries_match(entry, entry2) for entry2 in history)


MergeActionType = Enum("touch", "copy", "move", "delete", "undelete",
                       "update", "update_history", "conflict")

## details is type-specific
# for copy: source
@type_constructors(MergeActionType)
class MergeAction(Record("type", "path", "older", "newer", "details")):
    def __new__(cls, type, older, newer, details = None):
        path = get_path(older, newer)
        return cls.new(type, path, older, newer, details)

# yields MergeAction
def calculate_merge_actions(source_entries, dest_entries):
    source_history_by_path = group_history_by_path(source_entries)
    dest_history_by_path = group_history_by_path(dest_entries)
    dest_latests_by_hash = \
            latests_by_hash_from_history_by_path(dest_history_by_path)

    for (diff, path, newer, older) in \
            diff_histories(source_history_by_path, dest_history_by_path):
        if diff == HistoryDiffType.newer:
            if newer.deleted:
                yield MergeAction.delete(older, newer)
            elif (older != None and entries_contents_match(older, newer)):
                yield MergeAction.touch(older, newer)
            elif newer.hash and newer.hash in dest_latests_by_hash:
                local_sources = dest_latests_by_hash[newer.hash]
                yield MergeAction.copy(older, newer, local_sources)
            else:  # Not deleted, different content, and not available locally
                yield MergeAction.update(older, newer)
        elif diff == HistoryDiffType.history_conflict:
            if older.deleted:
                yield MergeAction.update_history(older, newer)
            else:
                yield MergeAction.touch(older, newer)
        elif diff == HistoryDiffType.conflict:
            yield MergeAction.conflict(older, newer)

def latests_by_hash_from_history_by_path(history_by_path):
    latests_by_hash = {}
    for path, history in history_by_path.iteritems():
        latest = history.latest
        if not latest.deleted:
            setdefault(latests_by_hash, latest.hash, set).add(latest)
    return latests_by_hash

def get_path(*entries):
    for entry in entries:
        if entry is not None:
            return entry.path
    return None
