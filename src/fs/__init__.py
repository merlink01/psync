# Copyright 2006 Uberan - All Rights Reserved

from FileHistoryStore import FileHistoryStore, FileHistoryEntry
from FileSystem import FileSystem, join_paths

# returns whether mtimes are within 1 sec of each other, because
# Windows shaves off a bit of mtime info.
def mtimes_eq(mtime1, mtime2):
    return abs(mtime1 - mtime2) < 2
