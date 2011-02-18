# Copyright 2006 Uberan - All Rights Reserved

import sqlite3

from util import Record, sql, into, setdefault

TABLE_NAME = "files"
TABLE_FIELDS = ["path", "utime", "size", "mtime", "hash"]

class FileHistoryEntry(Record("path", "utime", "size", "mtime", "hash")):
    pass

class FileHistoryStore(Record("db_conn", "entries_by_peerid")):
    def __new__(cls, db_conn):
        create_table(db_conn)
        return cls.new(db_conn, {})

    #*** user peerid
    def read_entries(self, peerid = ""):
        return setdefault(self.entries_by_peerid, peerid,
                          select_entries, self.db_conn)

    #*** user peerid
    def add_entries(self, new_entries, peerid = ""):
        insert_entries(self.db_conn, new_entries)
        if peerid in self.entries_by_peerid:
            self.entries_by_peerid[peerid].extend(new_entries)

def create_table(db_conn):
    db_cursor = db_conn.cursor()
    try:
        db_cursor.execute(sql.create_table(
                "files", ["path varchar",
                          "utime integer",
                          "size integer",
                          "mtime integer",
                          "hash varchar"]))
        db_conn.commit()
    except sqlite3.OperationalError:
        pass  # Already exists?

# Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
# down to 40,000/sec, so that doesn't seem like a good idea.
@into(list)
def select_entries(db_conn):
    db_cursor = db_conn.cursor()
    for (path, utime, size, mtime, hash) in \
            db_cursor.execute(sql.select(TABLE_NAME, TABLE_FIELDS)):
        yield FileHistoryEntry(path, utime, size, mtime, hash)

def insert_entries(db_conn, entries):
    db_cursor = db_conn.cursor()
    insert_statement = sql.insert(TABLE_NAME, TABLE_FIELDS)
    for entry in entries:
        db_cursor.execute(insert_statement, entry)
    db_conn.commit()
