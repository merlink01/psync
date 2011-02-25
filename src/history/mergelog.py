# Copyright 2006 Uberan - All Rights Reserved

from util import Record, sql

TABLE_NAME = "merges"
TABLE_FIELD_TYPES = ["utime integer",
                     "peerid varchar",
                     "action varchar",
                     "groupid varchar",
                     "path varchar",
                     "details varchar",
                     "author_peerid varchar"]
TABLE_FIELDS = [ft.split(" ")[0] for ft in TABLE_FIELD_TYPES]

class MergeLogEntry(Record(*TABLE_FIELDS)):
    pass

class MergeLog(Record("db", "clock")):
    def __new__(cls, db, clock):
        db.create(TABLE_NAME, TABLE_FIELD_TYPES)
        return cls.new(db, clock)

    # return [entry]
    def read_entries(self, peerid):
        # *** use peerid
        return list(self.db.select(TABLE_NAME, TABLE_FIELDS,
                                   into=MergeLogEntry))

    def add_action(self, action):
        utime = self.clock.unix()
        peerid = action.newer.peerid
        action_type = str(action.type)
        (groupid, path) = action.gpath
        details = str(action.details) if action.details else ""
        author_peerid = action.newer.author_peerid
        entry = MergeLogEntry(utime, peerid, action_type,
                              groupid, path, details, author_peerid)

        self.db.insert(TABLE_NAME, TABLE_FIELDS, [entry])
