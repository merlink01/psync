# Copyright 2006 Uberan - All Rights Reserved

import operator

from util import Record, groupby

from store import TABLE_FIELDS

DELETED_SIZE = 0
DELETED_MTIME = 0

class History(Record("entries", "latest")):
    def __new__(cls, entries):
        # This is faster than max(history, key=HistoryEntry.get_utime)
        # by .2 secs for 150,000 entries.
        latest = max(entries)
        return cls.new(entries, latest)

    def __iter__(self):
        return iter(self.entries)

class HistoryEntry(Record(*TABLE_FIELDS)):
    @property
    def deleted(entry):
        return entry.mtime == DELETED_MTIME

    def get_gpath(entry):
        return (entry.groupid, entry.path)

    @property
    def gpath(entry):
        return (entry.groupid, entry.path)


def group_history_by_gpath(entries):
    # TODO: Make faster (give entry a cached gpath)?
    return groupby(entries, HistoryEntry.get_gpath, into=History)

def group_history_by_peerid(entries):
    return groupby(entries, operator.itemgetter(1), into=History)

