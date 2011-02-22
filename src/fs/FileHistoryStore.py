# Copyright 2006 Uberan - All Rights Reserved

import operator

from util import Record, sql, setdefault, groupby_set

DELETED_SIZE = 0
DELETED_MTIME = 0

TABLE_NAME = "files"
TABLE_FIELD_TYPES = ["utime integer",
                     "peerid varchar",
                     "path varchar",
                     "size integer",
                     "mtime integer",
                     "hash varchar",
                     "author_peerid varchar",
                     "author_utime integer"]
TABLE_FIELDS = [ft.split(" ")[0] for ft in TABLE_FIELD_TYPES]

## By putting utime first, we can shave .2-.3 seconds per 150,000
## entries when using latest_history_entry.
class FileHistoryEntry(Record(*TABLE_FIELDS)):
    @property
    def deleted(entry):
        return entry.mtime == DELETED_MTIME

def group_history_by_path(entries):
    return groupby_set(entries, operator.itemgetter(2))

## This is faster by .2 secs for 150,000 entries.
latest_history_entry = max
# def latest_history_entry(history):
#     return max(history, key = FileHistoryEntry.get_utime)

class FileHistoryStore(Record("db", "cache_by_peerid")):
    def __new__(cls, db):
        db.create(TABLE_NAME, TABLE_FIELD_TYPES)
        return cls.new(db, {})

    # return [entry]
    # TODO: safely return cached history_by_path or latest_by_path (thread safe)
    #*** use peerid
    def read_entries(self, peerid = ""):
        cached = setdefault(self.cache_by_peerid, peerid, lambda: \
                            HistoryCache.from_entries(self.select_entries()))
        # copy list for thread safety
        return list(cached.entries)

    # Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
    # down to 40,000/sec, so that doesn't seem like a good idea.
    def select_entries(self):
        return self.db.select(TABLE_NAME, TABLE_FIELDS, into=FileHistoryEntry)

    #*** use peerid
    def add_entries(self, new_entries, peerid = ""):
        self.db.insert(TABLE_NAME, TABLE_FIELDS, new_entries)
        cache = self.cache_by_peerid.get(peerid)
        if cache is not None:
            cache.add_entries(new_entries)

    # return [latest]
    #*** use peerid
    def read_latests_by_hashes(self, hashes, peerid = ""):
        cached = self.cache_by_peerid.get(peerid)
        if cached is not None:
            for path, latest in cached.latest_by_path:
                if latest.hash in hashes:
                    yield latest

class HistoryCache(Record("entries", "history_by_path", "latest_by_path")):
    @classmethod
    def from_entries(cls, entries):
        entries = list(entries)
        history_by_path = group_history_by_path(entries)
        latest_by_path = dict((path, latest_history_entry(history))
                              for path, history in history_by_path.iteritems())
        return cls.new(entries, history_by_path, latest_by_path)

    def add_entries(self, entries):
        self.entries.extend(entries)
        for entry in entries:
            history = setdefault(self.history_by_path, entry.path, set)
            history.add(entry)
            self.latest_by_path[entry.path] = latest_history_entry(history)
