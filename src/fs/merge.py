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


# The purpose of this file is to diff histories, determine what is
# different, and then merge the change from the source into the
# destination.  The only part that is left unhandled is "fetch", which
# is how we'll get the data of a file that needs to be updated.  The
# fetching may happen across the network.

from fs import join_paths
from history import MergeAction, MergeActionType, calculate_merge_actions
from util import groupby

# fetch is function of entry -> fetched_path, which will then be moved.
def diff_and_merge(source_history, dest_history, dest_groupids,
                   fetch, revisions, fs, history_store, peerid,
                   clock, merge_log, slog):
    touches, copies, moves, deletes, undeletes, updates, uphists, conflicts = \
             calculate_merge_actions(source_history, dest_history, revisions)

    # We must do copies and moves first, in case we change the source.
    for action in copies:
        source_latest = next(iter(action.details))
        source_path = verify_stat(fs, dest_groupids,
                                  source_latest.gpath, source_latest)
        dest_path = verify_stat(fs, dest_groupids,
                                action.gpath, action.older)
        with slog.copying(source_path, dest_path):
            fs.copy(source_path, dest_path, mtime = action.newer.mtime)
        merge(action, history_store, peerid, clock, merge_log, slog)
        
    for action in moves:
        source_latest = action.details
        source_path = verify_stat(fs, dest_groupids,
                                  source_latest.gpath, source_latest)
        dest_path = verify_stat(fs, dest_groupids,
                                action.gpath, action.older)
        with slog.moving(source_path, dest_path):
            fs.move(source_path, dest_path, mtime = action.newer.mtime)
        merge(action, history_store, peerid, clock, merge_log, slog)

    for action in uphists:
        merge(action, history_store, peerid, clock, merge_log, slog)

    for action in touches:
        dest_path = verify_stat(fs, dest_groupids,
                                action.gpath, action.older)
        fs.touch(dest_path, action.newer.mtime)
        merge(action, history_store, peerid, clock, merge_log, slog)

    for action in deletes:
        dest_path = get_dest_path(dest_groupids, action.gpath)
        if fs.exists(dest_path):
            dest_path = verify_stat(fs, dest_groupids,
                                    action.gpath, action.older)
            trash(dest_path, revisions, action.older, slog)
        merge(action, history_store, peerid, clock, merge_log, slog)

    for action in undeletes:
        rev_entry = action.details
        dest_path = verify_stat(fs, dest_groupids,
                                action.gpath, action.older)
        trash(dest_path, revisions, action.older, slog)
        with slog.untrashing(rev_entry, dest_path):
            revisions.copy_out(rev_entry, dest_path)
        merge(action, history_store, peerid, clock, merge_log, slog)

    for action in updates:
        # TODO: Allow fetch to be asyncronous somehow.
        source_path = fetch(action.newer)
        dest_path = verify_stat(fs, dest_groupids,
                                action.gpath, action.older)
        trash(dest_path, revisions, action.older, slog)
        # TODO: Move instead of copying once we have a real fetcher in place.
        with slog.copying(source_path, dest_path):
            fs.copy(source_path, dest_path, mtime = action.newer.mtime)
        merge(action, history_store, peerid, clock, merge_log, slog)

    
    # We're going to simply resolve conflicts by letting the newer
    # mtime win.  Since a deleted mtime is 0, a non-deleted always
    # wins over a deleted.  If the remove end is older, we copy it to
    # "revisions", so that if later it "wins", it's a local copy, and
    # so a user could potentially look at it.
    for action in conflicts:
        older, newer = action.older, action.newer
        # When mtimes are equal, use utime, size, and hash as tie-breakers.
        if (newer.mtime, newer.utime, newer.size, newer.hash) \
               > (older.mtime, older.utime, older.size, older.hash):
            updates.append(action)
        # TODO: pre-fetch losing conflicts into revisions?
        # else:
        #     source_path = fetch(action.newer)
        #     revisions.move_in(source_path, action_newer)


def get_dest_path(dest_groupids, gpath):
    (groupid, path) = gpath
    root = dest_groupids.to_root(groupid)
    # TODO: Handle this error better.
    if root is None:
        raise Exception("unknown groupid!", groupid)
    return join_paths(root, path)

def verify_stat(fs, dest_groupids, gpath, latest):
    full_path = get_dest_path(gpath)

    # TODO: Handle these errors better.
    if latest is None or latest.deleted:
        if fs.exists(full_path):
            raise Exception("file created!", full_path)
    else:
        if not fs.stat_eq(full_path, latest.size, latest.mtime):
            raise Exception("file changed!", full_path)

    return full_path
        
def trash(source_path, revisions, dest_entry, slog):
    if fs.exists(source_path):
        with slog.trashing(dest_entry):
            revisions.move_in(source_path, dest_entry)
            fs.remove_empty_parent_dirs(source_path)

def merge(action, history_store, peerid, clock, merge_log, slog):
    new_entry = action.newer.alter(utime=clock.unix(), peerid=peerid)
    history_store.add_entries([new_entry])
    merge_log.add_action(action.set_newer(new_entry))
    slog.merged(action)
