# Copyright 2006 Uberan - All Rights Reserved

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
        # *** use peerid when selecting entries
        cached = setdefault(self.cache_by_peerid, peerid, lambda: \
                            HistoryCache(list(self.select_entries())))
        # copy list for thread safety
        return list(cached.entries)

    # Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
    # down to 40,000/sec, so that doesn't seem like a good idea.
    def select_entries(self):
        return self.db.select(TABLE_NAME, TABLE_FIELDS, into=HistoryEntry)

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
