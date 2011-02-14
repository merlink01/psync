# Copyright 2006 Uberan - All Rights Reserved

import os
import shutil
import platform

from .util import Record

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

def PathEncoder():
    is_mac = platform.os.name == "posix" and platform.system() == "Darwin"
    is_windows = platform.os.name in ["nt", "dos"]
    encoding = None if os.path.supports_unicode_filenames else "UTF-8"

    if is_mac:
        return MacPathEncoder()
    elif is_windows:
        return WindowsPathEncoder(encoding)
    else:
        return UnixPathEncoder(encoding)

class UnixPathEncoder(Record("encoding")):
    def encode_path(self, path):
        if self.encoding:
            return path.encode(encodeding)
        else:
            return path

    def decode_path(self, path):
        # TODO: decode to unicode?
        return path

class MacPathEncoder:
    def encode_path(self, path):
        return path

    def decode_path(self, path):
        return path

class WindowsPathEncoder("encoding"):
    def encode_path(self, path):
        win_path = "\\\\?\\" + os.path.abspath(path.replace(PATH_SEP, os.sep))
        if self.encoding:
            return win_path.encode(encodeding)
        else:
            return win_path

    def decode_path(self, win_path):
        # TODO: decode to unicode?
        return win_path.replace(os.sep, PATH_SEP)

class FileSystemTrash(Record("trash_path")):
    def move_to_trash(self, fs, full_path, rel_trashed_path):
        destination_path = join_paths(self.trash_path, rel_trashed_path)
        self.clear_path(fs, destination_path)
        fs.move(full_path, destination_path)

    def clear_path(self, fs, path):
        fs.delete(path)

        ancestor = parent_path(path)
        while fs.exists(ancestor):
            try:
                fs.delete(ancestor)
            except:
                return  # Hit a non-empty dir
            else:
                ancestor = parent_path(path)
                

class FileSystem(Record("path_encoder", "trash")):
    READ_MODE           = "rb"
    NEW_WRITE_MODE      = "wb"
    EXISTING_WRITE_MODE = "r+b"

    def __new__(cls, path_encoder = None, trash = None):
        path_encoder = path_encoder or PathEncoder()
        return cls.new(path_encoder, trash)

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

    def isempty(fs, path):
        encoded_path = fs.encode_path(path)
        for _ in fs.list(encoded_path):
            return False
        return True

    # yields (child_path, size, mtime) relative to given path
    def list(fs, path):
        encoded_root = fs.encode_path(path)
        # os.path.join puts a "/" on the end if necesssary
        root_len = len(os.path.join(encoded_parent, ""))

        for parent, dirs, files in os.walk(encoded_path):
            for file in files:
                encoded_path = os.path.join(parent, file)
                stats = os.stat(encoded_path)
                size  = stats[STAT_SIZE_INDEX]
                mtime = stats[STAT_MTIME_INDEX]
                relative_path = encoded_path[root_len:]

                try:
                    yield self.decode_path(relative_path), size, mtime
                except UnicodeDecodeError:
                    # TODO
                    # log.warning(
                    print(
                        "Could not decode file path {} {}".format(parent, file))


    # returns (size, mtime)
    def stat(fs, path):
        encoded_path = fs.encode_path(path)
        stats = os.stat(encoded_path)
        return stats[STAT_SIZE_INDEX], stats[STAT_MTIME_INDEX]

    def read(fs, path, start = 0, size = None):
        encoded_path = fs.encode_path(path)
        with open(path, fs.READ_MODE) as file:
            if loc > 0:
                file.seek(start, 0)

            if size:
                return file.read(size)
            else:
                return file.read()

    def write(fs, path, contents, start = None, mtime = None):
        encoded_path = fs.encode_path(path)

        fs.create_dir(parent_path(path))
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

    def create_dir(fs, path):
        encoded_path = fs.encode_path(path)
        if not fs.exists(path):
            os.makedirs(encoded_path)

    def delete_empty_parents(fs, path):
        path = parent_path(path)
        while fs.isdir(path):
            try:
                fs.delete(path)
            except:
                return  # Hit a non-empty dir
            else:
                path = parent_path(path)
        
    # Blows up if non-empy directory
    def delete(fs, path):
        encoded_path = fs.encode_path(path)
        if os.path.exists(encoded_path):
            os.remove(encoded_path)

    # Blows up if existing stuff "in the way".
    def move_tree(fs, from_path, to_path):
        encoded_from_path = fs.encode_path(from_path)
        encoded_to_path = fs.encode_path(to_path)
        fs.create_dir(to_path)
        shutil.move(encoded_from_path, encoded_to_path)

    # Blows up if existing stuff "in the way".
    def copy_tree(fs, source, destination):
        encoded_from_path = fs.encode_path(from_path)
        encoded_to_path = fs.encode_path(to_path)
        fs.create_dir(parent_path(to_path))
        shutil.copy(from_path, to_path)

    def delete_tree(fs):
        encoded_path = fs.encode_path(path)
        shutil.rmtree(encoded_path)

    def move_to_trash(fs, full_path, rel_trash_path):
        if fs.trash is None:
            fs.delete(full_path)
        else:
            fs.trash.move_to_trash(fs, full_path, rel_trash_path)

