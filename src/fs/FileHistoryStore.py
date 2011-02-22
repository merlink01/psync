# Copyright 2006 Uberan - All Rights Reserved

import operator

from fs import DELETED_SIZE, DELETED_MTIME
from util import Record, sql, setdefault, groupby_set

TABLE_NAME = "files"
TABLE_FIELD_TYPES = ["utime integer",
                     "peerid varchar",
                     "path varchar",
                     "size integer",
                     "mtime integer",
                     "hash varchar",
                     "author_peerid varchar",
                     "author_utime integer",
                     "author_action varchar"]
TABLE_FIELDS = [ft.split(" ")[0] for ft in TABLE_FIELD_TYPES]

## By putting utime first, we can shave .2-.3 seconds per 150,000
## entries when using latest_history_entry.
class FileHistoryEntry(Record(*TABLE_FIELDS)):
    @property
    def deleted(entry):
        return entry.mtime == DELETED_MTIME

def group_history_by_path(entries):
    return groupby_set(entries, operator.itemgetter(2))

def group_history_by_peerid(entries):
    return groupby_set(entries, operator.itemgetter(1))

## This is faster by .2 secs for 150,000 entries.
latest_history_entry = max
# def latest_history_entry(history):
#     return max(history, key = FileHistoryEntry.get_utime)

class FileHistoryStore(Record("db", "slog", "cache_by_peerid")):
    def __new__(cls, db, slog):
        db.create(TABLE_NAME, TABLE_FIELD_TYPES)
        return cls.new(db, slog, {})

    # return [entry]
    def read_entries(self, peerid):
        cached = setdefault(self.cache_by_peerid, peerid, lambda: \
                            HistoryCache(list(self.select_entries())))
        # copy list for thread safety
        return list(cached.entries)

    # Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
    # down to 40,000/sec, so that doesn't seem like a good idea.
    def select_entries(self):
        return self.db.select(TABLE_NAME, TABLE_FIELDS, into=FileHistoryEntry)

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
