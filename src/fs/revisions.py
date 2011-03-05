# Copyright (c) 2012, Peter Thatcher
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   2. Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   3. The name of the author may not be used to endorse or promote products
#      derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This file has two purposes:
#  1. Provide a way to have a "trash" with multiple versions in it.
#  2. Provide a way to copy things back out of the trash to avoid
#  transferring files across the network if we have it in the "trash".
#  We do so by using the hash as a kind of id of the version.  If
#  hashing is turned off (the hash == ""), we use the modtime+size as
#  the version id, which is hopefully good enough.

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

    # TODO: Use the entry.groupid in the revision_path.  Right now,
    # all of the groups are kind of merged together, which is kind of
    # messy.
    def get_revision_path(self, entry):
        path, ext = os.path.splitext(entry.path)
        if entry.hash:
            return "{path}_{hash}{ext}".format(
                path = path, hash = entry.hash, ext = ext)
        else:
            return "{path}_{size}_{mtime}{ext}".format(
                path = path, size = entry.size, mtime = entry.mtime, ext = ext)
            

            
        
        
        

                
                
            
