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


# The purpose of this file is to define a history entry.  The fields
# are what are necessary to do a proper scan/diff/merge:
#
# utime: The time we record an entry.  Needed for efficient copying of
#   entries between machines (we can ask "give me the entries since the
#   last utime I asked").
#
# peerid: Not techincally need for scan/diff/merge, but needed to
#   store all of the histories of different peers in one table.
#
# groupid: The group the path is relative to.  It there is only one
#   group, this isn't strictly needed.
#
# path: relative to the group, in cross-platform way (with "/" separator)
#
# size: number of bytes of the file.  Used to detect if the size has
#   changed, the file has changed.  But, the file may change and the
#   size not change, so we need to check more than the size.
#
# mtime: mtime on the file system.  If it changed, the file may have
#   changed, but maybe not.  Watch out: Windows shaves off a bit.
#  
# hash: hex-encoded hash digest.  Right now, we use a sha1.  The value
#   might also be empty, which would disable certain features.
#
# author_peerid and author_utime: The peerid and utime of when the
#   file was discovered.  This is crucial for knowing that a
#   particular file version is the same between different peers,
#   without relying on just the contents (which wouldn't allow undoing
#   changes to be replicated).
#
# author_action: "create", "delete", or "change", a convenience value
#   so we can know why the author updated the history.

import operator

from util import Record, groupby

from store import TABLE_FIELDS

DELETED_SIZE = 0
DELETED_MTIME = 0

class History(Record("entries", "latest")):
    def __new__(cls, entries):
        # Using max(entries) is faster than
        # max(history, key=HistoryEntry.get_utime)
        # by .2 secs for 150,000 entries.
        latest = max(entries)
        return cls.new(entries, latest)

    def __iter__(self):
        return iter(self.entries)

# To avoid having two lists of fields that could get out of sync, we
# use the canonical list which we keep in the list of db fields.
class HistoryEntry(Record(*TABLE_FIELDS)):
    @property
    def deleted(entry):
        return entry.mtime == DELETED_MTIME

    def get_gpath(entry):
        return GroupedPath(entry.groupid, entry.path)

    @property
    def gpath(entry):
        return GroupedPath(entry.groupid, entry.path)

class GroupedPath(Record("groupid", "path")):
    """Like a FileSystem RootedPath, but relative to a group, not a
    FileSystem path.  Often known as a "gpath".  """
    pass

def group_history_by_gpath(entries):
    # TODO: Make faster (give entry a cached gpath)?
    return groupby(entries, HistoryEntry.get_gpath, into=History)

def group_history_by_peerid(entries):
    return groupby(entries, operator.itemgetter(1), into=History)

