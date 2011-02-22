# Copyright 2006 Uberan - All Rights Reserved

from util import Record, sql, into, setdefault

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

## This is faster by .2 secs for 150,000 entries.
latest_history_entry = max
# def latest_history_entry(history):
#     return max(history, key = FileHistoryEntry.get_utime)

class FileHistoryStore(Record("db", "entries_by_peerid")):
    def __new__(cls, db):
        db.create(TABLE_NAME, TABLE_FIELD_TYPES)
        return cls.new(db, {})

    #*** user peerid
    def read_entries(self, peerid = ""):
        return setdefault(self.entries_by_peerid, peerid, self.select_entries)

    # Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
    # down to 40,000/sec, so that doesn't seem like a good idea.
    @into(list)
    def select_entries(self, peerid = ""):
        return self.db.select(TABLE_NAME, TABLE_FIELDS, into=FileHistoryEntry)

    #*** user peerid
    def add_entries(self, new_entries, peerid = ""):
        self.db.insert(TABLE_NAME, TABLE_FIELDS, new_entries)
        if peerid in self.entries_by_peerid:
            self.entries_by_peerid[peerid].extend(new_entries)
