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

# The purpose of this file is to compare histories (sets of entries)
# and determine what actions would be necessary to merge the history
# of one into the history of the other.

from entry import History, HistoryEntry, group_history_by_gpath
from util import Record, Enum, type_constructors, setdefault

# A history conflict means that's there's a conflict, but the contents
# are the same, so we don't have to re-transfer anything: just figure
# out how to set the history right.
HistoryDiffType = Enum("insync", "newer", "older",
                       "conflict", "history_conflict")

# @type_constructors So you can say HistoryDiff.newer(latest1, latest1)
@type_constructors(HistoryDiffType)
class HistoryDiff(Record("type", "gpath", "latest1", "latest2")):
    """Compares the latest of the histories of a given gpath (GroupedPath)."""
    def __new__(cls, type, latest1, latest2):
        gpath = get_gpath(latest1, latest2)
        return cls.new(type, gpath, latest1, latest2)
    
# yields all HistoryDiffs
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

# True is size and hash are the same, even if other metadata is
# different.
def entries_contents_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.hash  == entry2.hash)

# True only if entries are exactly the same, including author peerid
# and author_utime.  This is crucial for allowing two instances of the
# same file to have different versions.  For example, if someone
# changes a file, but I don't like the change, so I change it BACK.
# In that case, it would have the same contents hash, size, and
# possibly mtime as the old version, but the utime would be different.
def entries_match(entry1, entry2):
    return (entry1.size  == entry2.size and
            entry1.mtime == entry2.mtime and
            entry1.hash  == entry2.hash and
            entry1.author_peerid == entry2.author_peerid and
            entry1.author_utime == entry2.author_utime)
        
def has_matching_entry(history, entry):
    return any(entries_match(entry, entry2) for entry2 in history)


# Touch when the mtime is the only thing different.
# Copy when there's a local copy of the data available (no transfer needed).
# Undelete when there's a copy in the trash we can get.
# Move when there's a local copy that is also being deleted,
#   so a move would be faster than a copy and a delete.
# Update_history when only the history differs, and not the contents.
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
# First, found the source entries which are newer or in conflict.  Then:
#  if newer is deleted, delete
#  if newer is the same contents, touch
#  if newer hash is the copy of something deleted, move
#  if newer hash already exists locally, copy
#  if newer hash is in the trash, undelete
#  if history conflict: resolve by updating local history
#  if conflict: pass along as a conflict
#  otherwise: update
def calculate_merge_actions_without_moves(source_entries, dest_entries,
                                          revisions):
    actions = calculate_merge_actions(source_entries, dest_entries, revisions)
    action_by_type = groupby(actions, MergeAction.get_type)
    touches, copies, moves, deletes, undeletes, updates, uphists, conflicts = \
             (action_by_type.get(type, []) for type in MergeActionType)
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
            moves.append(action.alter(
                type = MergeActionType.move, details = delete.older))
        else:
            unmoved_copies.append(action)
    copies = unmoved_copies

    return (touches, copies, moves, deletes, undeletes,
            updates, uphists, conflicts)

def iter_merge_actions_without_moves(source_entries, dest_entries, revisions):
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
            # Not deleted, different content, and not available locally
            else:
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
