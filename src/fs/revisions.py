# Copyright 2006 Uberan - All Rights Reserved

import os

from fs import join_paths
from util import Record

class RevisionStore(Record("fs", "root")):
    def __contains__(self, entry):
        full_path = self.get_full_revision_path(entry)
        return self.fs.stat_eq(full_path, entry.size, entry.mtime)

    def move_in(self, source_path, dest_entry):
        dest_path = self.get_full_revision_path(dest_entry)
        self.fs.move(source_path, dest_path, mtime = dest_entry.mtime)

    def copy_out(self, source_entry, dest_path):
        source_path = self.get_full_revision_path(source_entry)
        self.fs.copy(source_path, dest_path, mtime = source_entry.mtime)

    def get_full_revision_path(self, entry):
        return join_paths(self.root, self.get_revision_path(entry))

    def get_revision_path(self, entry):
        path, ext = os.path.splitext(entry.path)
        if entry.hash:
            return "{path}_{hash}{ext}".format(
                path = path, hash = entry.hash, ext = ext)
        else:
            return "{path}_{size}_{mtime}{ext}".format(
                path = path, size = entry.size, mtime = entry.mtime, ext = ext)
            

            
        
        
        

                
                
            
