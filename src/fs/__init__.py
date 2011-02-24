# Copyright 2006 Uberan - All Rights Reserved

# Windows shaves off a bit of mtime info.
# TODO: Only do this sillyness on Windows.
def mtimes_eq(mtime1, mtime2):
    return (mtime1 >> 1) == (mtime2 >> 1)

from FileSystem import FileSystem, FileStat, join_paths
from PathFilter import PathFilter
from scan import scan_and_update_history


