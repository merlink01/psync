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

# The purpose of this file is to abstactly access the FileSystem,
# especially for the purpose of scanning it to see what files are
# different.  It works really hard to do so fast.

import hashlib
import logging
import os
import platform
import shutil
import sys

from util import Record

DELETED_SIZE = 0
DELETED_MTIME = 0

class RootedPath(Record("root", "rel")):
    """ Represents a path (rel) that is relative to another path
    (root).  For examples, when scanning a large directory, it is
    convenient to know the paths relative to the directory passed in.
    In code a RootedPath is often called an "rpath"."""

    @property
    def full(self):
        return join_paths(*self)

# An "rpath" is short for "RootedPath"
class FileStat(Record("rpath", "size", "mtime")):
    @classmethod
    def from_deleted(cls, rpath):
        return cls.new(rpath, DELETED_SIZE, DELETED_MTIME)

    @property
    def deleted(entry):
        return entry.mtime == DELETED_MTIME

STAT_SIZE_INDEX  = 6
STAT_MTIME_INDEX = 8
 
# All paths are unicode separated by "/".  We encode for a given
# platform (Windows) as necessary.
PATH_SEP = "/"

def join_paths(*paths):
    return PATH_SEP.join(paths)

def parent_path(path):
    try:
        parent, child = path.rsplit(PATH_SEP, 1)
    except:
        parent, child = "", path
    return parent

# Windows shaves off a bit of mtime info.
# TODO: Only do this sillyness on Windows.
def mtimes_eq(mtime1, mtime2):
    return (mtime1 >> 1) == (mtime2 >> 1)

# Path encoding is needed because Windows has really funky rules for
# dealing with unicode paths. It seems like an all OSes, what you get
# back and what it expects from you isn't consistent.  The PathEncoder
# stuff is there to be a single place where we can take care of this.
# Also, we want to deal with paths in a consistent way with "/" and
# not worry about Windows oddities ("\", etc).
def PathEncoder():
    is_mac = platform.os.name == "posix" and platform.system() == "Darwin"
    is_windows = platform.os.name in ["nt", "dos"]
    decoding = sys.getfilesystemencoding()
    encoding = None if os.path.supports_unicode_filenames else decoding

    if is_windows:
        return WindowsPathEncoder(encoding, decoding)
    else:
        return UnixPathEncoder(encoding, decoding)

class UnixPathEncoder(Record("encoding", "decoding")):
    def encode_path(self, path):
        if self.encoding:
            return path.encode(self.encoding)
        else:
            return path

    def decode_path(self, path):
        return path.decode(self.decoding)

class WindowsPathEncoder(Record("encoding", "decoding")):
    def encode_path(self, path):
        win_path = "\\\\?\\" + os.path.abspath(path.replace(PATH_SEP, os.sep))
        if self.encoding:
            return win_path.encode(self.encoding)
        else:
            return win_path

    def decode_path(self, win_path):
        return win_path.replace(os.sep, PATH_SEP).decode(self.decoding)

class FileSystem(Record("slog", "path_encoder")):
    """Encapsulates all of the operations we need on the FileSystem.
    The most important part is probably listing/stating."""

    READ_MODE           = "rb"
    NEW_WRITE_MODE      = "wb"
    EXISTING_WRITE_MODE = "r+b"

    # slog needs to have 
    def __new__(cls, slog):
        return cls.new(slog, PathEncoder())

    def encode_path(fs, path):
        return fs.path_encoder.encode_path(path)

    def decode_path(fs, path):
        return fs.path_encoder.decode_path(path)

    def exists(fs, path):
        encoded_path = fs.encode_path(path)
        return os.path.exists(encoded_path)

    def isdir(fs, path):
        encoded_path = fs.encode_path(path)
        return os.path.isdir(encoded_path)

    def isfile(fs, path):
        encoded_path = fs.encode_path(path)
        return os.path.isfile(encoded_path)

    def isempty(fs, path):
        encoded_path = fs.encode_path(path)
        for _ in fs.list(encoded_path):
            return False
        return True

    # yields FileStat, with same "root marker" rules as self.list(...)
    #
    # On my 2008 Macbook, reads about 10,000 files/sec when doing small
    # groups (5,000 files), and 4,000 files/sec when doing large
    # (200,000).  These means it can take anywhere from .1 sec to 1
    # minute.  Cacheing seems to improve performance by about 30%.
    # While running, the CPU is pegged :(.  Oh well, 60,000 files in 8
    # sec isn't too bad.  That's my whole home directory.
    #
    # On my faster linux desktop machine, it's about 30,000 files/sec
    # when cached, even for 200,00 files, which is a big improvement.
    def list_stats(fs, root, root_marker = None, names_to_ignore = frozenset()):
        return fs.stats(fs.list(
            root, root_marker = root_marker, names_to_ignore = names_to_ignore))

    # yields a RootedPath for each file found in the root.  The intial
    # root is the given root.  Deeper in, if there is a "root_marker"
    # file in a directory, that directory becomes a new root.
    def list(fs, root, root_marker = None, names_to_ignore = frozenset()):
        listdir = os.listdir
        join = os.path.join
        isdir = os.path.isdir
        islink = os.path.islink

        def decode(encoded_path):
            try:
                return fs.decode_path(encoded_path)
            except Exception as err:
                fs.slog.path_error("Could not decode file path {0}: {1}"
                                   .format(repr(encoded_path)), err)
                return None

        # We pass root around so that we only have to decode it once.
        def walk(root, encoded_root, encoded_parent):
            child_names = listdir(encoded_parent)
            if root_marker is not None:
                if root_marker in child_names:
                    encoded_root = encoded_parent
                    root = decode(encoded_root)

            # If decoding root fails, no point in traversing any futher.
            if root is not None:
                for child_name in child_names:
                    if child_name not in names_to_ignore:
                        encoded_full = join(encoded_parent, child_name)
                        if isdir(encoded_full):
                            if not islink(encoded_full):
                                for child in \
                                        walk(root, encoded_root, encoded_full):
                                    yield child
                        else:
                            rel = decode(encoded_full[len(encoded_root)+1:])
                            if rel:
                                yield RootedPath(root, rel)

        encoded_root = fs.encode_path(root)
        return walk(root, encoded_root, encoded_root)

    # yields FileStats
    def stats(fs, rpaths):
        stat = os.stat
        for rpath in rpaths:
            try:
                encoded_path = fs.encode_path(rpath.full)
                stats = stat(encoded_path)
                size = stats[STAT_SIZE_INDEX]
                mtime = stats[STAT_MTIME_INDEX]
                yield FileStat(rpath, size, mtime)
            except OSError:
                pass  # Probably a link

    # returns (size, mtime)
    def stat(fs, path):
        encoded_path = fs.encode_path(path)
        stats = os.stat(encoded_path)
        return stats[STAT_SIZE_INDEX], stats[STAT_MTIME_INDEX]

    # Will not throw OSError for no path.  Will return False in that case.
    def stat_eq(fs, path, size, mtime):
        try:
            (current_size, current_mtime) = fs.stat(path)
            return (current_size == size and
                    mtimes_eq(current_mtime, mtime))
        except OSError:
            return False
        

    def read(fs, path, start = 0, size = None):
        encoded_path = fs.encode_path(path)
        with open(path, fs.READ_MODE) as file:
            if loc > 0:
                file.seek(start, 0)

            if size:
                return file.read(size)
            else:
                return file.read()

    # On my 2008 Macbook, with SHA1, it can hash 50,000 files
    # totalling 145GB (about 3MB each file) in 48min, which is 17
    # files totalling 50MB/sec.  So, if you scan 30GB of new files, it
    # will take 10min.  During that time, CPU usage is ~80%.
    def hash(fs, path, hash_type = hashlib.sha1, chunk_size = 100000):
        if hash_type == None:
            return ""

        hasher = hash_type()
        for chunk_data in fs._iter_chunks(path, chunk_size):
            hasher.update(chunk_data)
        return hasher.digest()

    def _iter_chunks(fs, path, chunk_size):
        encoded_path = fs.encode_path(path)
        with open(path, fs.READ_MODE) as file:
            chunk = file.read(chunk_size)
            while chunk:
                yield chunk
                chunk = file.read(chunk_size)

    def write(fs, path, contents, start = None, mtime = None):
        encoded_path = fs.encode_path(path)

        fs.create_parent_dirs(path)
        if (start is not None) and fs.exists(encoded_path):
            mode = fs.EXISTING_WRITE_MODE
        else:
            mode = fs.NEW_WRITE_MODE

        with open(encoded_path, mode) as file:
            if start is not None:
                file.seek(start, 0)
                assert start == file.tell(), \
                       "Failed to seek to proper location in file"

            file.write(contents)

        if mtime is not None:
            fs.touch(encoded_path, mtime)

    def touch(fs, path, mtime):
        encoded_path = fs.encode_path(path)
        os.utime(encoded_path, (mtime, mtime))

    def create_parent_dirs(fs, path):
        fs.create_dir(parent_path(path))

    def create_dir(fs, path):
        encoded_path = fs.encode_path(path)
        if not os.path.exists(encoded_path):
            os.makedirs(encoded_path)

    # # Blows up if existing stuff "in the way".
    def move(fs, from_path, to_path, mtime = None):
        encoded_from_path = fs.encode_path(from_path)
        encoded_to_path = fs.encode_path(to_path)
        fs.create_parent_dirs(to_path)
        os.rename(encoded_from_path, encoded_to_path)
        if mtime is not None:
            fs.touch(to_path, mtime)

    # Blows up if existing stuff "in the way".
    def copy(fs, from_path, to_path, mtime = None):
        encoded_from_path = fs.encode_path(from_path)
        encoded_to_path = fs.encode_path(to_path)
        fs.create_parent_dirs(to_path)
        shutil.copyfile(encoded_from_path, encoded_to_path)
        if mtime is not None:
            fs.touch(to_path, mtime)

    # Blows up if non-empy directory
    def delete(fs, path):
        encoded_path = fs.encode_path(path)
        if os.path.exists(encoded_path):
            os.remove(encoded_path)

    def remove_empty_parent_dirs(fs, path):
        encoded_parent_path = fs.encode_path(parent_path(path))
        try:
            os.removedirs(encoded_parent_path)
        except OSError:
            pass  # Not empty
