# Copyright 2006 Uberan - All Rights Reserved

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


