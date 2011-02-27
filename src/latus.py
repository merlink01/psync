# Copyright 2006 Uberan - All Rights Reserved

import sys
if sys.version_info < (2, 6):
    raise UnsupportedPythonVersionError(sys.version)

from contextlib import contextmanager

import hashlib
import os
import sqlite3
import traceback

from fs import (FileSystem, PathFilter, RevisionStore,
                join_paths, scan_and_update_history)
from history import (HistoryStore, MergeAction, MergeActionType, MergeLog,
                     calculate_merge_actions)
from util import (Record, Clock, MockFuture, RunTime, SqlDb,
                  groupby, flip_dict, start_thread)
                  
class StatusLog:
    def __init__(self, clock):
        self.clock = clock
        
    def log(self, *args):
        print args

    def time(self, name):
        return RunTime(name, self.clock, self.log_run_time)

    def log_run_time(self, rt):
        self.log("timed", "{0:.2f} secs".format(rt.elapsed),
                 rt.name, rt.result)

    def actor_error(self, name, err, trace):
        self.log("actor error", name, err)
        traceback.print_exception(type(err), err, trace)

    def actor_died(self, name, err, trace):
        self.log("actor died", name, err)
        traceback.print_exception(type(err), err, trace)

    def actor_finished(self, name):
        self.log("actor finished", name)

    def path_error(self, err):
        self.log("path error", err)

    def ignored_rpaths(self, rpaths):
        for rpath in rpaths:
            self.log("ignored", rpath)

    def ignored_rpath_without_groupid(self, gpath):
        self.log("ignore rpath without groupid", gpath)

    def ignored_gpath_without_root(self, gpath):
        self.log("ignore gpath without root", gpath)

    def not_a_file(self, path):
        self.log("not a file", path)

    def could_not_hash(self, path):
        self.log("could not hash", path)

    def inserted_history(self, entries):
        for entry in entries:
            self.log("inserted", entry)

    def merged(self, action):
        self.log("merged", action)

    @contextmanager
    def hashing(self, path):
        self.log("begin hashing", path)
        yield
        self.log("end hashing", path)

    @contextmanager
    def copying(self, from_path, to_path):
        self.log("begin copy", from_path, to_path)
        yield
        self.log("end copy", from_path, to_path)

    @contextmanager
    def moving(self, from_path, to_path):
        self.log("begin move", from_path, to_path)
        yield
        self.log("end move", from_path, to_path)

    @contextmanager
    def trashing(self, entry):
        details = entry.hash or (entry.size, entry.mtime)
        self.log("begin trashing", entry.path, details)
        yield
        self.log("end trashing", entry.path, details)

    @contextmanager
    def untrashing(self, entry, dest_path):
        details = entry.hash or (entry.size, entry.mtime)
        self.log("begin untrashing", dest_path, entry.path, details)
        yield
        self.log("end untrashing", dest_path, entry.path, details)

# *** implement reading .latusconf
class Groupids(Record("root_by_groupid", "groupid_by_root")):
    def __new__(cls, root_by_groupid):
        groupid_by_root = flip_dict(root_by_groupid)
        return cls.new(root_by_groupid, groupid_by_root)

    def to_root(self, groupid):
        return self.root_by_groupid.get(groupid, None)

    def from_root(self, root):
        return self.groupid_by_root.get(root, None)


# fetch is entry -> future(fetched_path)
# trash is (full_path, entry) -> ()
# merge is action -> ()
# *** handle errors, especially unknown groupid, created! and changed! errors
def diff_fetch_merge(source_entries, source_groupids,
                     dest_entries, dest_groupids, dest_store,
                     fetch, trash, merge, revisions, fs, slog):
    def get_dest_path(gpath):
        (groupid, path) = gpath
        root = dest_groupids.to_root(groupid)
        if root is None:
            raise Exception("unknown groupid!", groupid)
        return join_paths(root, path)

    def verify_stat(gpath, latest):
        full_path = get_dest_path(gpath)

        if latest is None or latest.deleted:
            if fs.exists(full_path):
                raise Exception("file created", full_path)
        else:
            if not fs.stat_eq(full_path, latest.size, latest.mtime):
                raise Exception("file changed!", full_path)

        return full_path
        
    actions = calculate_merge_actions(source_entries, dest_entries, revisions)
    action_by_type = groupby(actions, MergeAction.get_type)
    touches, copies, moves, deletes, undeletes, updates, uphists, conflicts = \
             (action_by_type.get(type, []) for type in MergeActionType)

    # We're going to simply resolve conflicts by letting the newer
    # mtime win.  Since a deleted mtime is 0, a non-deleted always
    # wins over a deleted.  If the remove end is older, we copy it to
    # "revisions", so that if later it "wins", it's a local copy, and
    # so a user could potentially look at it.
    for action in conflicts:
        older, newer = action.older, action.newer
        # When mtimes are equal, use utime, size, and hash as tie-breakers.
        if (newer.mtime, newer.utime, newer.size, newer.hash) \
               > (older.mtime, older.utime, older.size, older.hash):
            updates.append(action)
        else:
            # TODO: Make sure fetching goes into "revisions".
            fetch(action.newer)

    # If a copy also has a matching delete, make it as "move".
    deletes_by_hash = groupby(deletes, \
            lambda delete: delete.older.hash if delete.older else None)
    real_copies = []
    for action in copies:
        deletes_of_hash = deletes_by_hash.get(action.newer.hash, [])
        if action.newer.hash and deletes_of_hash:
            # Pop so we only match a given delete once.  But we
            # leave the deleted in the deleteds so that it's put
            # in the history and merge data, but we don't put it
            # in the revisions.
            delete = deletes_of_hash.pop()
            moves.append(action.alter(
                type = MergeActionType.move, details = delete.older))
        else:
            real_copies.append(action)
    copies = real_copies

    # We must do copy and move actions first, in case we change the souce.
    for action in copies:
        source_latest = next(iter(action.details))
        source_path = verify_stat(source_latest.gpath, source_latest)
        dest_path = verify_stat(action.gpath, action.older)
        with slog.copying(source_path, dest_path):
            fs.copy(source_path, dest_path, mtime = action.newer.mtime)
        merge(action)
        
    for action in moves:
        source_latest = action.details
        source_path = verify_stat(source_latest.gpath, source_latest)
        dest_path = verify_stat(action.gpath, action.older)
        with slog.moving(source_path, dest_path):
            fs.move(source_path, dest_path, mtime = action.newer.mtime)
        merge(action)

    for action in uphists:
        merge(action)

    for action in touches:
        dest_path = verify_stat(action.gpath, action.older)
        fs.touch(dest_path, action.newer.mtime)
        merge(action)

    for action in deletes:
        dest_path = get_dest_path(action.gpath)
        if fs.exists(dest_path):
            dest_path = verify_stat(action.gpath, action.older)
            trash(dest_path, action.older)
        merge(action)

    for action in undeletes:
        rev_entry = action.details
        dest_path = verify_stat(action.gpath, action.older)
        trash(dest_path, action.older)
        with slog.untrashing(rev_entry, dest_path):
            revisions.copy_out(rev_entry, dest_path)
        merge(action)

    for action in updates:
        fetch(action.newer).wait()  # ***: More proper fetching.
        source_path = source_path_f.get()
        dest_path = verify_stat(action.gpath, action.older)
        trash(dest_path, action.older)
        with slog.copying(source_path, dest_path):
            fs.copy(source_path, dest_path, mtime = action.newer.mtime)
        merge(action)
    
def filter_entries_by_gpath(entries, groupids, path_filter):
    return (entry for entry in entries
            if (groupids.to_root(entry.groupid) is not None and
                not path_filter.ignore_path(entry.path)))

# python latus.py ../test1 pthatcher@gmail.com/test1 ../test2 pthatcher@gmail.com/test2
def main_sync_two(args, conf):
    clock = Clock()
    slog = StatusLog(clock)
    fs = FileSystem(slog)

    fs_root1, peerid1, fs_root2, peerid2 = args
    db_path1 = os.path.join(fs_root1, conf.db_path)
    db_path2 = os.path.join(fs_root2, conf.db_path)
    revisions_root2 = os.path.join(fs_root2, conf.revisions_path)

    groupids1 = Groupids({"group1": fs_root1,
                          "group1/cmusic": os.path.join(
                              fs_root1, "Conference Music")})
    groupids2 = Groupids({"group1": fs_root2,
                          "group1/cmusic": os.path.join(
                              fs_root2, "cmusic")})

    fs.create_parent_dirs(db_path1)
    fs.create_parent_dirs(db_path2)
    with sqlite3.connect(db_path1) as db1, sqlite3.connect(db_path2) as db2:
        history_store1 = HistoryStore(SqlDb(db1), slog)
        history_store2 = HistoryStore(SqlDb(db2), slog)
        revisions2 = RevisionStore(fs, revisions_root2)
        merge_log2 = MergeLog(SqlDb(db2), clock)

        history_entries1 = scan_and_update_history(
            fs, fs_root1, conf.root_mark, conf.path_filter, conf.hash_type,
            history_store1, peerid1, groupids1, clock, slog)
        history_entries2 = scan_and_update_history(
            fs, fs_root2, conf.root_mark, conf.path_filter, conf.hash_type,
            history_store2, peerid2, groupids2, clock, slog)

        def fetch(entry):
            root = groupids1.to_root(entry.groupid)
            return MockFuture(join_paths(root, entry.path))

        def trash(source_path, dest_entry):
            if fs.exists(source_path):
                with slog.trashing(dest_entry):
                    revisions2.move_in(source_path, dest_entry)
                    fs.remove_empty_parent_dirs(source_path)

        def merge(action):
            new_entry = action.newer.alter(utime=clock.unix(), peerid=peerid2)
            history_store2.add_entries([new_entry])
            slog.merged(action)
            merge_log2.add_action(action.set_newer(new_entry))

        history_entries1 = filter_entries_by_gpath(
            history_entries1, groupids2, conf.path_filter)
        diff_fetch_merge(history_entries1, groupids1,
                         history_entries2, groupids2, history_store2,
                         fetch, trash, merge, revisions2, fs, slog)

        # for log_entry in sorted(merge_log2.read_entries(peerid2)):
        #    print log_entry

def main_actor_test():
    from util import Actor, async, async_result, AllFuture

    class ThreadedActorStarter:
        def __init__(self):
            self.actors = []

        def start(self, actor):
            start_thread(actor.run, actor.name)
            self.actors.append(actor)
            return actor

        def stop_all(self):
            return AllFuture([actor.stop() for actor in self.actors])

    class StatusLogActor(Actor, StatusLog):
        def __init__(self, name, clock):
            StatusLog.__init__(self, clock)
            Actor.__init__(self, name, self)

        @async
        def log(self, *args):
            print args

    class Peer(Actor):
        def __init__(self, peerid, slog):
            self.peerid = peerid
            Actor.__init__(self, repr(peerid), slog)

        def __repr__(self):
            return "{0.__class__.__name__}({0.peerid})".format(self)

        @async
        def scan(self):
            self.slog.log("scan", self.peerid)

        @async_result
        def read_entries(self):
            return [(self.peerid, "entry1")]

        @async_result
        def read_chunk(self, hash, loc, size):
            return ("chunk", self.peerid, hash, loc, size)

        def finish(self):
            self.slog.log("finish", self.peerid)

    clock = Clock()
    starter = ThreadedActorStarter()
    #slog = StatusLog(clock)
    slog = starter.start(StatusLogActor("StatusLog", clock))
    peer1 = starter.start(Peer("pthatcher@gmail.com/test1", slog))
    peer2 = starter.start(Peer("pthatcher@gmail.com/test2", slog))
    
    peer1.scan()
    peer2.scan()
    chunk1_f = peer1.read_chunk("hash", 0, 100)
    print peer2.read_chunk("hash", 100, 100).wait(0.1)
    print chunk1_f.wait(0.1)

    #import code
    #code.interact("Debug Console", local = {"peer1" : peer1, "peer2": peer2})
    starter.stop_all().wait(0.2)


class Config:
    hash_type = hashlib.sha1

    root_mark = ".latusconf"
    db_path = ".latus/latus.db"
    revisions_path = ".latus/revisions/"

    names_to_ignore = frozenset([
        # don't scan our selves!
        ".latus",

        # Mac OSX things we shouldn't sync, mostly caches and trashes
        "Library", ".Trash", "iPod Photo Cache", ".DS_Store",

        # Unix things we shouldn't sync, mostly caches and trashes
        ".m2", ".ivy2", ".fontconfig", ".thumbnails", "thumbs.db",
        ".abobe", ".dvdcss", ".cache", ".macromedia", ".xsession-errors",
        ".mozilla", ".java", ".gconf", ".gconfd", ".kde", ".nautilus", ".local",
        ".icons", ".themes",

        # These are debatable.
        ".hg", ".git", ".evolution"])

    # For 5 patterns, the initial scan time is doubled.  But, that
    # time is dwarfed by the hash time anyway.  The path filter can
    # memoize to make subsequen scans just as fast as unfiltered ones.
    globs_to_ignore = \
        [# Parallels big files we probably should never sync
         "*.hds", "*.mem",

         # Contents that change a lot, but we wouldn't want to sync
         ".config/google-chrome/*", ".config/google-googletalkplugin/*",

         # emacs temp files, which we probably never care to sync
         "*~", "*~$", "~*.tmp"]

    path_filter = PathFilter(globs_to_ignore, names_to_ignore)   



if __name__ == "__main__":
    #main_sync_two(sys.argv[1:], Config)
    #main_actor_test()
    
    import code
    import sys
    import time

    from xml.etree import cElementTree as ET
    from thirdparty import sleekxmpp

    FILES_NS = "{latus}files"

    class XmppClient:
        def __init__(self, jid, password, server_address):
            self.server_address = server_address
            self.client = sleekxmpp.ClientXMPP(jid, password)
            for event, handler in ((attr[3:], getattr(self, attr))
                                   for attr in dir(self)
                                   if attr.startswith("on_")):
                self.client.add_event_handler(event, handler)
            self.jid = None
            
            self.client.register_handler(
                sleekxmpp.Callback("Latus files IQ",
                    sleekxmpp.MatchXPath("{jabber:client}iq/%s" % (FILES_NS,)),
                    self.on_files_iq))

        def run(self):
            if self.client.connect(self.server_address):
                self.client.process(threaded=False)
            else:
                raise Exception("failed to connect")            

        def on_connected(self, _):
            print ("connected", )

        def on_failed_auth(self, _):
            print ("failed auth", )
            # TODO: make it stop :)

        def on_session_start(self, _):
            print ("session start", self.client.boundjid.full)
            self.client.get_roster()
            # print ("got roster", self.client.roster)
            ## If you don't do this, you wan't appear online.
            self.client.send_presence()
            print ("address", self.client.address)
            self.jid = self.client.boundjid.full

        def on_disconnected(self, _):
            print ("disconnected", )

        def on_got_online(self, presence):
            remote_jid = presence["from"].full
            if remote_jid.endswith("6c617473"):
                print (self.jid, "online", remote_jid)

        def on_got_offline(self, presence):
            remote_jid = presence["from"].full
            if remote_jid.endswith("6c617473"):
                print (self.jid, "offline", remote_jid)

        def on_files_iq(self, iq):
            print (self.jid, "files iq", iq.xml[0])

        def send_get_files(self, tojid, since):
            iq = self.client.make_iq()
            iq["to"] = tojid
            iq["type"] = "get"
            iq["id"] = "TODO"  # ***
            iq.append(ET.Element(FILES_NS, {"since": str(1234)}))
            iq.send()
            # *** return Future for the parsed response
            

    password = sys.argv[1]
    server_address = ("talk.google.com", 5222)

    client1 = XmppClient("pthatcher@gmail.com/test2_6c617473",
                         password, server_address)

    client2 = XmppClient("pthatcher@gmail.com/test1_6c617473",
                         password, server_address)
    
    start_thread(client1.run, "XmppClient 1")
    start_thread(client2.run, "XmppClient 2")
    time.sleep(1.0)
    client1.send_get_files(client2.jid, 1234)

    code.interact("Debug Console", local = {
        "client1": client1,
        "client2": client2})
        
    # notes:
    # for gmail, the last 8 lettes must be hex.  If they are, you can
    # be as long as you want.  If they are not, you have to be only 10
    # chars.  So, basically you get 10 chars, and have to have 8 hex
    # chars after that.
    #
    # connected({}) fires when connected to server (not authed)
    # disconnected({}) fires when disconnected from server
    # failed_auth({}) fires when auth is wrong
    # got_online and got_offline are useful
    # presence has (from .getStanzaValues)
    #   status, from, show, priority, to, type, id
    #   from and type seem useful.
    #     type can be "available" and "away"
    # client.roster is
    #    {name: {name: ...,
    #            presence: {status: '',
    #                       priority: 24,  # ???
    #                       show: 'available'},
    #            in_roster: True,
    #            groups: [],
    #            subscription: both}}
    # client seems to have:
    #   .send_xml(xml)
    #   .send_message(to, body,
    #   .make_iq_result(id)
    #   .make_iq_get()
    #   .make_iq_set()
    #     then do iq.append(?)
    #  
