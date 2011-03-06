# Copyright (c) 2011, Peter Thatcher
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


# The purpose of this file is to store the history of files
# persistantly on disk.  We use sqlite do do so because it is fast and
# easy.  We read data of disk lazily, and then cache it.  Writing back
# to disk is incremental and also very fast.

from util import Record, sql, setdefault

TABLE_NAME = "files"
## By putting utime first, we can shave .2-.3 seconds per 150,000
## entries when getting latest by using max().
TABLE_FIELD_TYPES = ["utime integer",
                     "peerid varchar",
                     "groupid varchar",
                     "path varchar",
                     "size integer",
                     "mtime integer",
                     "hash varchar",
                     "author_peerid varchar",
                     "author_utime integer",
                     "author_action varchar"]
TABLE_FIELDS = [ft.split(" ")[0] for ft in TABLE_FIELD_TYPES]

# Have to import HistoryEntry after TABLE_FIELDS is set, since
# HistoryEntry is based on TABLE_FIELDS.
from entry import HistoryEntry, group_history_by_peerid

class HistoryStore(Record("db", "slog", "cache_by_peerid")):
    def __new__(cls, db, slog):
        db.create(TABLE_NAME, TABLE_FIELD_TYPES)
        return cls.new(db, slog, {})

    # return [entry]
    def read_entries(self, peerid):
        cached = setdefault(self.cache_by_peerid, peerid, lambda: \
                            HistoryCache(list(self.select_entries(peerid))))
        # copy list for thread safety
        return list(cached.entries)

    # Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
    # down to 40,000/sec, so that doesn't seem like a good idea.
    def select_entries(self, peerid):
        # TODO: use a where clause when selecting by peerid.  It will
        # be a lot faster!
        return (entry for entry in 
                self.db.select(TABLE_NAME, TABLE_FIELDS, into=HistoryEntry)
                if entry.peerid == peerid)

    def add_entries(self, new_entries):
        self.db.insert(TABLE_NAME, TABLE_FIELDS, new_entries)
        self.slog.inserted_history(new_entries)

        for peerid, new_entries in \
                group_history_by_peerid(new_entries).iteritems():
            cache = self.cache_by_peerid.get(peerid)
            if cache is not None:
                cache.add_entries(new_entries)

class HistoryCache(Record("entries")):
    def add_entries(self, entries):
        self.entries.extend(entries)
