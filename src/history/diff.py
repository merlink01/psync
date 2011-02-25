# Copyright 2006 Uberan - All Rights Reserved

from entry import History, HistoryEntry, group_history_by_gpath
from util import Record, Enum, type_constructors, setdefault

HistoryDiffType = Enum("insync", "newer", "older",
                       "conflict", "history_conflict")

# So you can say HistoryDiff.newer(latest1, latest1)
@type_constructors(HistoryDiffType)
class HistoryDiff(Record("type", "gpath", "latest1", "latest2")):
    def __new__(cls, type, latest1, latest2):
        gpath = get_gpath(latest1, latest2)
        return cls.new(type, gpath, latest1, latest2)
    
# yield HistoryDiff
def diff_histories(history_by_gpath1, history_by_gpath2):
    for gpath1, history1 in history_by_gpath1.iteritems():
        history2 = history_by_gpath2.get(gpath1)
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

    for gpath2, history2 in history_by_gpath2.iteritems():
        history1 = history_by_gpath1.get(gpath2)
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
class MergeAction(Record("type", "gpath", "older", "newer", "details")):
    def __new__(cls, type, older, newer, details = None):
        gpath = get_gpath(older, newer)
        return cls.new(type, gpath, older, newer, details)

# yields MergeAction
def calculate_merge_actions(source_entries, dest_entries, revisions):
    source_history_by_gpath = group_history_by_gpath(source_entries)
    dest_history_by_gpath = group_history_by_gpath(dest_entries)
    dest_latests_by_hash = \
            latests_by_hash_from_history_by_gpath(dest_history_by_gpath)
    dest_entries_by_hash = \
            entries_by_hash_from_history_by_gpath(dest_history_by_gpath)

    for (diff, gpath, newer, older) in \
            diff_histories(source_history_by_gpath, dest_history_by_gpath):
        if diff == HistoryDiffType.newer:
            if newer.deleted:
                yield MergeAction.delete(older, newer)
            elif (older != None and entries_contents_match(older, newer)):
                yield MergeAction.touch(older, newer)
            elif newer.hash and newer.hash in dest_latests_by_hash:
                local_sources = dest_latests_by_hash[newer.hash]
                yield MergeAction.copy(older, newer, local_sources)
            else:  # Not deleted, different content, and not available locally
                # But maybe it's in the trash!
                revision_entry = first(rev_entry for rev_entry in
                                       dest_entries_by_hash.get(newer.hash, [])
                                       if rev_entry in revisions)
                if revision_entry:
                    yield MergeAction.undelete(older, newer, revision_entry)
                else:
                    yield MergeAction.update(older, newer)
        elif diff == HistoryDiffType.history_conflict:
            if older.deleted:
                yield MergeAction.update_history(older, newer)
            else:
                yield MergeAction.touch(older, newer)
        elif diff == HistoryDiffType.conflict:
            yield MergeAction.conflict(older, newer)

def latests_by_hash_from_history_by_gpath(history_by_gpath):
    latests_by_hash = {}
    for history in history_by_gpath.itervalues():
        latest = history.latest
        if not latest.deleted:
            setdefault(latests_by_hash, latest.hash, set).add(latest)
    return latests_by_hash

def entries_by_hash_from_history_by_gpath(history_by_gpath):
    entries_by_hash = {}
    for history in history_by_gpath.itervalues():
        for entry in history:
            if entry.hash:
                setdefault(entries_by_hash, entry.hash, set).add(entry)
    return entries_by_hash


def get_gpath(*entries):
    for entry in entries:
        if entry is not None:
            return entry.gpath
    return None

def first(vals, default = None):
    for val in vals:
        return val
    return default
