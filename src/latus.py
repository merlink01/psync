# Copyright 2006 Uberan - All Rights Reserved

import sqlite3
import sys
import time

from fs import FileSystem

class Clock:
  def now_unix(self):
    return time.time()

DELETED_SIZE = 0
DELETED_MTIME = 0

# returns whether mtimes are within 1 sec of each other, because
# Windows shaves off a bit of mtime info.
def mtimes_eq(mtime1, mtime2):
  return abs(mtime1 - mtime2) < 2

#*** add peerid, groupid
#*** give a *set* of directories to ignore
#  it's fast, and should make scanning faster
#*** add filtering by full path name, but *memoize*
#*** do filtering after scanning and diffing, but before inseting into history
#*** after fetching and merging, just let metadata scanner take care of updating history.  Only "inject" things when we resolve a conflict by using the local to win.
#  only have to check filter for ones changed
#  can read attributes for Win32, too
# returns bool: created db or not
def create_file_history_db(db_conn):
  db_cursor = db_conn.cursor()
  try:
    db_cursor.execute(
      """create table files (path varchar,
                             utime integer,
                             size integer,
                             mtime integer)""")
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
  for (path, utime, size, mtime) in db_cursor.execute(
    """select path, utime, size, mtime from files"""):
    history = history_by_path.get(path)
    latest = (utime, size, mtime)
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
      """insert into files (path, utime, size, mtime)
         values (?, ?, ?, ?)""", entry)
  db_conn.commit()

# yields (change, path, size, mtime, history)
def scan_diff(fs, root, history_by_path, detect_missing = True):
  if detect_missing:
    missing_paths = set(history_by_path.iterkeys())
  else:
    missing_paths = []

  for path, size, mtime in fs.list_stats(root):
    history = history_by_path.get(path)
    if history is None:
      yield ("created", path, size, mtime, history)
    else:
      (latest_utime, latest_size, latest_mtime) = max(history)
      if size != latest_size or not mtimes_eq(mtime, latest_mtime):
        yield ("changed", path, size, mtime, history)
      else:
        yield ("unchanged", path, size, mtime, history)

    if detect_missing:
      missing_paths.discard(path)

  for path in missing_paths:
    yield ("deleted", path, DELETED_SIZE, DELETED_MTIME, history)

if __name__ == "__main__":
  root = sys.argv[1]
  db_path = sys.argv[2]

  clock = Clock()
  fs = FileSystem()

  with sqlite3.connect(db_path) as db_conn:
    create_file_history_db(db_conn)

    history_by_path = read_file_history_from_db(db_conn)
    print ("read history", len(history_by_path))

    new_utime = int(clock.now_unix())
    new_history_entries = []  # (path, utime, size, mtime)
    for (change, path, size, mtime, history) in \
        scan_diff(fs, root, history_by_path):
      # print (change, path, size, mtime, history)
      if change != "unchanged":
        # TODO: if history[-1].utime > new_utime, make sure to
        # increase utime.  Otherwise reseting the clock will mess
        # things up.
        # *** wait and then rescan for the changed ones
        # *** hash the file
        new_history_entries.append((path, new_utime, size, mtime))
    print ("scanned fs", len(new_history_entries))

    insert_file_history_entries_into_db(db_conn, new_history_entries)
    print ("inserted", len(new_history_entries))

    history_by_path2 = read_file_history_from_db(db_conn)
    print ("read history2", len(history_by_path2))

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
#     async_names = ["list_stats", "read", "write", "create_dir",
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

#import gevent
## Doesn't seem to work welll with sleekxmpp :(
# from gevent import monkey
# monkey.patch_all()
# 
# Maybe try this:
# class GeventXmpp(sleekxmpp.ClientXMPP):
#   def process(self):
#     gevent.spawn(self._event_runner)
#     gevent.spawn(self._send_thread)
#     gevent.spawn(self._process)

# directory = HttpJsonLatusDirectory(
#   config.directoryName, config.directoryURL, 
#   crypto.deserialize_public_key(config.directoryPublicKey),
#   LatusFileCache(services.fs, config.latusCacheDirectory))
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

# gevent:
# from gevent.server import StreamServer
# server = StreamServer(('0.0.0.0', 6000), lambda (socket, address): ...)
# server.server_forever()
# ...
# socket.makefile().write("foo")
# socket.makefile().read()
#
# from gevent.socket import wait_write
# 


