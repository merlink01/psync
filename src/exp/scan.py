# Copyright (c) 2011, Peter Thatcher
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

import os
import re
import itertools

from ..util import Record, approx_eq

class FileStat(Record("size", "mtime", "hidden", "system")):
    # Windows stores mtime with a precision of 2, so we have to
    # compare mtimes in a funny way
    def __eq__(this, that):
        return (isinstance(that, this.__class__) and 
                this.size == that.size and
                #this.hidden == that.hidden and
                #this.system == that.system and
                approx_eq(this.mtime, that.mtime, delta = 2))

class FileInfo(Record("digest", "chunks")):
    pass

class FileChunk(Record("loc", "size", "digest")):
    pass


class FileScanSettings(Record("chunk_size", "stabilization_delay")):
    pass


def stat_path(path):
    # use both fs.stat and fs.read_attributes (hidden + system)
    pass  #***

def scan(paths, chunk_size):
    #***
    return (self.keyedVersionFromFileSystemDetails(details) for details in
            self.fs.listDetails(base_path) if details is not None)

def read_info(path, expected_stat):
    assert expected_stat == stat_path(path)
    stream = fs.readStream(path)
    file_info = ... # *** hash file while hashing chunks.  See fs.hashFile?
                    # using settings.chunk_size
    assert expected_stat == stat_path(path)
    return file_info

def stable_scan(paths, chunk_size, stablization_delay):
    kvs1 = scan(paths, chunk_size)
    sleep(stabilization_delay)
    kvs2 = {kv.key: kv for kv in scan(paths)}
    return [kv1 for kv1 in kv1 if kv1 == kvs2[kv.key]]

def dir_start(children):
    pass  #***

def read_chunk(path, chunk):
    data = fs.read(path, chunk.size, offset = chunk.loc)
    assert chunk.size == len(data)
    assert chunk.digest == hash_digest(data)
    return data


