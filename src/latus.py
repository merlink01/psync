# Copyright 2006 Uberan - All Rights Reserved

import fnmatch
import hashlib
import re
import sqlite3
import sys
import time

from fs import FileSystem, join_paths

#*** memoize and test performance impact of paths_to_ignore
#*** do double-check after hashing (re-stat)
#*** more logging (instead of printing)

#*** add peerid to files.db
#*** add more filtring
#  after scanning and diffing, but before inseting into history
#  memoize
#  can read attributes for Win32, too
#*** add gropuid
#*** after fetching and merging, just let metadata scanner take care
#    of updating history.  Only "inject" things when we resolve a conflict
#    by using the local to win.
#*** commit in chunks of 1,000 so files start syncing even while scanning the first time (which will take a long time)

def main(fs_path, db_path):
  clock = Clock()
  fs = FileSystem()
  #hash_type = hashlib.sha1
  hash_type = hashlib.md5
  #hash_type = None
  names_to_ignore = {"Library", ".Trash", "iPod Photo Cache",
                     ".m2", ".ivy2", ".fontconfig", ".thumbnails",
                     ".hg", ".git",
                     ".DS_Store"}
  patterns_to_ignore = {re.compile(fnmatch.translate(pattern), re.IGNORECASE)
                        for pattern in 
                        # "*.mp3", "*.jpg", "*.mp4"
                        ["*.hds", "*.mem", "*.vob", "*.mp4"]}
  
  with sqlite3.connect(db_path) as db_conn:
    create_file_history_db(db_conn)

    history_by_path = read_file_history_from_db(db_conn)
    print ("read history", len(history_by_path))

    new_utime = int(clock.now_unix())
    new_history_entries = []  # (path, utime, size, mtime)
    #  also, should compile regexes ahead of time
    for (change, path, size, mtime, history) in \
        scan_and_diff(fs, fs_path, names_to_ignore, history_by_path):
      if any(pattern.match(path) for pattern in patterns_to_ignore):
        change = "ignored"  #*** make a better name?

      #if change != "unchanged":
      #  print (change, path, size, mtime, history)
      if change == "ignored":
        print (change, path, size, mtime, history)
      # TODO: make an enum?
      if change == "created" or change == "changed":
        # TODO: make a FileSystem that always knows how to do prepend a path?
        print ("hash", path)
        hash = fs.hash(join_paths(fs_path, path)).encode("hex")
        print ("hashed", path, hash)
        new_history_entries.append((path, new_utime, size, mtime, hash))
      elif change == "deleted":
        new_history_entries.append((path, new_utime, size, mtime, hash))
        hash = ""
    print ("scanned fs", len(new_history_entries))

    #*** use iterator, and maybe just combine with scan_and_diff
    for (path, new_utime, new_size, new_mtime) in new_history_entries:
      pass

    insert_file_history_entries_into_db(db_conn, new_history_entries)
    print ("inserted", len(new_history_entries))

    history_by_path2 = read_file_history_from_db(db_conn)
    print ("read history2", len(history_by_path2),
           sum(max(history)[1] for history in history_by_path2.itervalues()))
      

# yields (change, path, size, mtime, history)
def scan_and_diff(fs, root, names_to_ignore, history_by_path):
  missing_paths = set(history_by_path.iterkeys())
  for path, size, mtime in fs.list_stats(root, names_to_ignore):
    missing_paths.discard(path)
    # *** do filtering here?
    # *** use enum for change type?

    history = history_by_path.get(path)
    if history is None:
      yield ("created", path, size, mtime, history)
    else:
      (latest_utime, latest_size, latest_mtime, latest_hash) = max(history)
      if size != latest_size or not mtimes_eq(mtime, latest_mtime):
        yield ("changed", path, size, mtime, history)
      else:
        yield ("unchanged", path, size, mtime, history)

  for path in sorted(missing_paths):
    yield ("deleted", path, DELETED_SIZE, DELETED_MTIME, history)




class Clock:
  def now_unix(self):
    return time.time()

DELETED_SIZE = 0
DELETED_MTIME = 0

# returns whether mtimes are within 1 sec of each other, because
# Windows shaves off a bit of mtime info.
def mtimes_eq(mtime1, mtime2):
  return abs(mtime1 - mtime2) < 2

# returns bool: created db or not
def create_file_history_db(db_conn):
  db_cursor = db_conn.cursor()
  try:
    db_cursor.execute(
      """create table files (path varchar,
                             utime integer,
                             size integer,
                             mtime integer,
                             hash varchar)""")
    db_conn.commit()
    return True
  except sqlite3.OperationalError:
    # Already exists?
    return False

# returns {path : [(utime, size, mtime)], with entires UNSORTED
#
# Reads 100,000/sec on my 2008 Macbook.  If you sort by utime, it goes
# down to 40,000/sec, so that doesn't seem like a good idea.
def read_file_history_from_db(db_conn):
  history_by_path = {}

  db_cursor = db_conn.cursor()
  for (path, utime, size, mtime, hash) in db_cursor.execute(
    """select path, utime, size, mtime, hash from files"""):
    history = history_by_path.get(path)
    latest = (utime, size, mtime, hash)
    if history is None:
      history_by_path[path] = [latest]
    else:
      history.append(latest)

  return history_by_path

# takes [(path, utime, size, mtime)]
def insert_file_history_entries_into_db(db_conn, new_history_entries):
  db_cursor = db_conn.cursor()
  
  for entry in new_history_entries:
    db_cursor.execute(
      """insert into files (path, utime, size, mtime, hash)
         values (?, ?, ?, ?, ?)""", entry)
  db_conn.commit()

if __name__ == "__main__":
  root = sys.argv[1]
  db_path = sys.argv[2]

  main(root, db_path)

# class FileScanner(Actor):
#     def __init__(self, fs):
#         self.fs = fs

#     @async
#     def scan(self, path):
#         for (child_path, size, mtime) in self.fs.list_stats(path):
#             if self.stopped:
#                 raise ActorStopped()
#         # ...
#         # Now, how do we setup periodic things?

# class AsyncFileSystem(ActorProxy):
#*** put list, stats, and list stats into a list (instead of iterator)
#     async_names = ["list", "stats", "list_stats",
#                    "read", "write", "create_dir",
#                    "move_tree", "copy_tree", "delete_tree"]
#     sync_names = ["exists", "isdir", "isempty", "stat",
#                   "touch", "move_to_trash"]
    
## possible XMPP:
# <iq type="get">
#   <files since=...>
#   <chunk hash=... loc=... size=...>
# <iq type="result">
#   <files>
#     <file path=... mtime=... size=... utime=...>
#   <chunk hash=... loc=... size=...>
#   

## For permissions, we need:
# (peerid, groupid, prefix?, can_read, can_write, is_owner)
# what is a groupid?
# do we use prefix?
# where is this stored?

## gevent server:
# from gevent.server import StreamServer
# server = StreamServer(('0.0.0.0', 6000), lambda (socket, address): ...)
# server.server_forever()
# ...
# socket.makefile().write("foo")
# socket.makefile().read()
#
# from gevent.socket import wait_write
# 

## Trying to get gevent and sleekxmpp to work together
#import gevent
# from gevent import monkey
# monkey.patch_all()
# 
# Maybe try this:
# class GeventXmpp(sleekxmpp.ClientXMPP):
#   def process(self):
#     gevent.spawn(self._event_runner)
#     gevent.spawn(self._send_thread)
#     gevent.spawn(self._process)

## From old ShareEver code
# private_ip, private_port = (Utils.get_local_ip(), config.connectionPort)
# peer = Peer(directory)
# network = Network(map_port_description = "ShareEver")
# fs = get_file_system(config.useOSTrash, config.customTrashFolder)
# log = ???

# connection_listener = ThreadedSocketServerListener(
#  "connection listener", config.connectionPort)
# rendezvous_listener = ThreadedSocketServerListener(
#   "rendezvous listener", config.rendezvousPort)
# start/stop:
#  peer, connection_listener, rendezvous_listener

# TODO: write persistent info (not config!)
#  connection port
#  peerid
#  crypto keys?
# TODO: debug consoloe: code.interact(
#  "ShareEver Debugging Console", local = {"peer" : peer}

