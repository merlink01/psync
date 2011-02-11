# Copyright 2006 Uberan - All Rights Reserved

import os
import re
import itertools

from ..util import Record, approx_eq

class FileInfo(Record("size", "mtime", "hidden", "system")):
    # Windows stores mtime with a precision of 2, so we have to
    # compare mtimes in a funny way
    def __eq__(this, that):
        return (isinstance(that, this.__class__) and 
                this.size == that.size and
                #this.hidden == that.hidden and
                #this.system == that.system and
                approx_eq(this.mtime, that.mtime, delta = 2))

    def __ne__(this, that):
        return not this == that

class FileValue(ISimplifyable):
    def __init__(self, hashes):
        self.hashes = hashes

    def __hash__(self):
        return hash(self.hashes)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.hashes)

    def __eq__(this, that):
        if isinstance(that, this.__class__):
            for this_hash in this.hashes:
                for that_hash in that.hashes:
                    if this_hash == that_hash:
                        return True
        return False

    def __ne__(this, that):
        return not this == that

    def toValues(self):
        return (self.hashes,)

    def toNamedValues(self):
        yield "hashes", self.hashes

    @classmethod
    def fromValues(cls, values):
        (hashes,) = values
        return cls(hashes)

    @classmethod
    def fromNamedValues(cls, named_values):
        hashes = dict(named_values).get("hashes", ())
        return cls(hashes)

def file_key_from_path(path):
    return Path(path).toCanonical(FILE_KEY_SEPERATOR)

def file_key_to_path(key):
    return Path.fromCanonical(key, FILE_KEY_SEPERATOR)

file_path_from_key = file_key_to_path


PREP_KEY_SEPARATOR = "_"
PREP_LOG_SUFFIX    = ".log"

class FilePreparedAttachmentVersion(Record("mtime")):
    pass

class KeyedVersionsMayBeStabilized(Record("space", "keyedVersions", "includesAllKeys", "sender")):
    pass


## NOTE: right now we only support versions in the PreparedAttachmentKey that are strings
def file_prep_key_from_path(path):
    file_path, version = path.rsplit(PREP_KEY_SEPARATOR, 1)
    file_key = file_key_from_path(file_path)
    version  = version[:len(version)-len(PREP_LOG_SUFFIX)]
    return PreparedAttachmentKey(file_key, version)

def file_prep_key_to_path(prep_key):
    file_key, version = prep_key
    file_path = file_key_to_path(file_key)
    return PREP_KEY_SEPARATOR.join([file_path, version])

def file_prep_key_to_log_path(prep_key):
    return file_prep_log_path_from_file_prep_path(file_prep_key_to_path(prep_key))

def file_prep_log_path_from_file_prep_path(file_prep_path):
    return file_prep_path + PREP_LOG_SUFFIX

def is_file_prep_log_path(file_prep_path):
    return file_prep_path.endswith(PREP_LOG_SUFFIX)

class FileSystemNodeSettings(Record("stabilizationDelay", "hashTypes", "attachmentSize", "preparedAttachmentsCompresionSize", "protectedName")):
    pass

class FileSystemNode(Actor, ASourceNode, ATargetNode, ReadableNodeMixIn, WritableNodeMixIn):
    handles, REGISTRY = make_handles_registry(ReadableNodeMixIn.REGISTRY, WritableNodeMixIn.REGISTRY)

    def __init__(self, address, settings, timer, watcher_class, *args, **kargs):
        store = FileSystemStore(address, settings, *args, **kargs)
        ReadableNodeMixIn.__init__(self, store)
        WritableNodeMixIn.__init__(self, address, store)

        self.typeid   = NodeType.File
        self.address  = address
        self.settings = settings
        self.timer    = timer

        self.watcherClass     = watcher_class
        self.producer         = None
        self.watcherByGroupid = FactoryDict(self.spawnWatcher)

    def makeEmptyPreparedAttachments(self, version):
        return self.store.makeEmptyPreparedAttachments(version)

    def act(self, producer):
        self.producer = producer
        self.processMessages()

    def spawnWatcher(self, groupid, base_path):
        if self.producer is None:
            raise ValueError("can't spawn a watcher until we have a producer")
        else:
            watcher = self.watcherClass(groupid, base_path, self)
            proxy   = self.producer.spawn("File Watcher for %r" % groupid, watcher)
            return watcher # we need to be able to call "stop"
 
    def stop(self, reason = None, last_messages = ()):
        Active.stop_all(self.getWatchers())
        Actor.stop(self, reason, last_messages)

    def waitUntilStopped(self, timeout = None):
        return Active.wait_until_all_stopped(self.getWatchers(), timeout) and Actor.waitUntilStopped(self, timeout)

    def getWatchers(self):
        return self.watcherByGroupid.values()
       

    def getSpaces(self):
        return tuple(self.spaceFromGroupid(groupid) for groupid in self.watcherByGroupid.iterkeys())

    @handles(PrepareSpace)
    def onPrepareSpace(self, message, sender):
        (space, is_empty) = message
        try:
            WritableNodeMixIn.onPrepareSpace(self, message, sender)
            self.activateWatcherIfNecessary(space)
        except SpaceUnavailable:
            pass #TODO: log error?

    @handles(GetKeyedVersions)
    def onGetKeyedVersions(self, message, sender):
        (space, keys) = message
        if space.subtype == NodeSubtype.Basic:
            try:
                self.addSubscriber(space, sender)
                keyed_versions    = tuple(self.iterKeyedVersions(space, keys))
                includes_all_keys = (keys is None)
                self.waitUntilStabilized(space, keyed_versions, includes_all_keys, sender)
            except SpaceUnavailable, error:
                self.sendTo(sender, repr(error))
        else:
            return ReadableNodeMixIn.onGetKeyedVersions(self, message, sender)

    @handles(SpacesInterested)
    def onSpacesInterested(self, message, sender):
        ReadableNodeMixIn.onSpacesInterested(self, message, sender)
        self.deactivateUninterestedWatchers() #spaces may have been removed

    @handles(FileChanged)
    def onFileChanged(self, (groupid, relative_path), sender):
        try:
            space          = self.spaceFromGroupid(groupid)
            key            = file_key_from_path(relative_path)
            #WARNING: WindowsError can happen here on Windows, so make sure we catch it!
            keyed_versions = tuple(self.iterKeyedVersions(space, (key,)))
            self.waitUntilStabilized(space, keyed_versions)
        except:
            pass #deal with it later, but don't blow up here

    def waitUntilStabilized(self, space, keyed_versions, includes_all_keys = False, sender = None):
        self.sendTo(self.timer, After(self.settings.stabilizationDelay,
                                      Deliver(self, KeyedVersionsMayBeStabilized(space, keyed_versions, includes_all_keys, sender))))

    @handles(KeyedVersionsMayBeStabilized)
    def onKeyedVersionsMayBeStabilized(self, (space, keyed_versions1, includes_all_keys, original_sender), sender):
        keyed_versions2           = tuple(self.iterKeyedVersions(space, ikeys(keyed_versions1)))
        stabilized_keyed_versions = tuple(kv1 if kv1 == kv2 else KeyedVersion(kv1.key, SyncableSpecial.Unstable)
                                          for kv1, kv2
                                          in outer_join(keyed_versions1, keyed_versions2, on = lambda keyed_version : keyed_version.key))
        keyed_versions_message    = KeyedVersions(space, stabilized_keyed_versions, includes_all_keys)

        if original_sender:
            self.sendTo(original_sender, keyed_versions_message)
        else:
            self.sendToInterested(space, keyed_versions_message)



        

    def spaceFromGroupid(self, groupid, subtype = NodeSubtype.Basic):
        return Space.fromDomainAndAddress(Domain(groupid, self.typeid), self.address, subtype)

    def groupidsFromSpaces(self, spaces):
        return (space.groupid for space in spaces
                if (space.address == self.address and space.typeid == self.typeid and space.subtype == NodeSubtype.Basic))
    
    
    def activateWatcherIfNecessary(self, space):
        base_path = self.store.getBasePathFromDomain(space.domain) #HACK: should access store like this
        self.watcherByGroupid.setdefault(space.groupid, base_path)
        

    def deactivateUninterestedWatchers(self):
        interested_groupids = frozenset(self.groupidsFromSpaces(self.iterInterestedSpaces()))
        active_groupids     = frozenset(self.watcherByGroupid.iterkeys())
        unneeded_groupids   = active_groupids - interested_groupids
        removed_watchers    = tuple(self.watcherByGroupid.remove(groupid) for groupid in unneeded_groupids)
        stop_and_wait_all(removed_watchers, 2.0) #TODO: log error if they aren't shutdown?

    @handles(PrepareAttachment)
    def onPrepareAttachment(self, (domain, attachment_key, attachment_value, source_version, source_address), sender):
        prepared_attachments, error = self.prepareAttachment(domain, attachment_key, attachment_value, source_version)
        self.sendTo(sender, PreparedAttachment(domain, attachment_key, prepared_attachments, source_address, error))
        if error is None:
            space            = Space.fromDomainAndAddress(domain, self.address, NodeSubtype.PreparedAttachment)
            compressed_space = space.setSubtype(NodeSubtype.CompressedPreparedAttachment)
            key              = PreparedAttachmentKey(attachment_key.key, attachment_key.version)
            self.sendKeyedVersionsToInterested(space,            (key,))
            self.sendKeyedVersionsToInterested(compressed_space, (key,))

    @handles(GetAttachmentStream)
    def onGetAttachmentStream(self, request, sender):
        try:
            domain, key, version = request
            value    = self.store.getAttachmentStream(domain, key, version)
            response = AttachmentStream(domain, key, value)
        except Exception, err:
            response = AttachmentStream(domain, key, None, err)
        self.sendTo(sender, response)

    @handles(WriteAttachments)
    def onWriteAttachments(self, request, sender):
        domain, key, version = request.domain, request.key, request.version
        try:
            new_kv = self.store.writeAttachments(domain, key, version, request.attachments)
            response = AttachmentsWritten(domain, key, version, new_kv.value, new_kv.version)
        except Exception, err:
            #import traceback
            #traceback.print_exc()
            response = AttachmentsWritten(domain, key, version, None, None, repr(err))
        self.sendTo(sender, response)

class FileSystemStore(ReadableNodeStore, IWritableNodeStore):
    def __init__(self, address, settings, services, group_mapper, hasher):
        self.address         = address
        self.settings        = settings
        self.services        = services
        self.fs              = services.fs
        self.groupMapper     = group_mapper
        self.hasher          = hasher

    ### IReadableNodeStore
    def getSpaces(self):
        raise NotImplementedError 

    def iterKeys(self, space):
        return (key for key, version in self.iterKeyedVersions(space))

    def getSpaceVersion(self, space):
        if space.subtype == NodeSubtype.Basic:
            with self.safeBasePath(space.domain) as base_path:
                return self.fs.dirid(base_path)
        else:
            return None 

    def iterKeyedVersions(self, space, keys = None):
        if keys is None:
            if space.subtype == NodeSubtype.PreparedAttachment or space.subtype == NodeSubtype.CompressedPreparedAttachment:
                return self.getPrepKeyedVersionsFromDomain(space.domain)
            else:
                return self.getKeyedVersionsFromDomain(space.domain)
        else:
            return ReadableNodeStore.iterKeyedVersions(self, space, keys)
                
    def getKeyedVersion(self, space, key):
        if space.subtype == NodeSubtype.PreparedAttachment or space.subtype == NodeSubtype.CompressedPreparedAttachment:
            return self.getPrepKeyedVersionFromDomainAndKey(space.domain, key)
        else:
            return self.getKeyedVersionFromDomainAndKey(space.domain, key)
            
    def getKeyedValue(self, space, key):
        if space.subtype == NodeSubtype.PreparedAttachment:
            return self.getPrepKeyedValueFromDomainAndKey(space.domain, key)
        elif space.subtype == NodeSubtype.CompressedPreparedAttachment:
            kv = self.getPrepKeyedValueFromDomainAndKey(space.domain, key)
            if kv.value is None:
                return kv
            else:
                return kv.setValue(kv.value.compress(self.settings.preparedAttachmentsCompresionSize))
        else:
            return self.getKeyedValueFromDomainAndKey(space.domain, key)

    def getAttachment(self, domain, attachment_key):
        if isinstance(attachment_key.version, FileVersion):
            return self.readAttachment(domain, attachment_key)
        else:
            return self.readPreparedAttachment(domain, attachment_key)
            
    def getAttachmentStream(self, domain, key, version):
        return self.readAttachmentStreamFromFullPath(self.getFullPath(domain, key, version), version)
        
    def writeAttachments(self, domain, key, version, attachments):
        full_path = self.getFullPath(domain, key, version)
        if attachments is None:
            self.maybeDeletePath(full_path)
        elif isinstance(attachments, str):
            bytes = attachments
            self.writeAttachmentsToFullPath(full_path, bytes)
        elif isinstance(attachments, unicode):
            source_path   = attachments
            #relative_path =  file_path_from_key(key)
            self.moveFile(domain, source_path, full_path)
        else:
            raise ValueError("incorrect attachments type: %r", attachments.__class__)

        return self.getKeyedValueFromDomainAndKey(domain, key)
        

    ### IWritableNodeStore
    def prepareSpace(self, space, is_empty):
        assert space.address == self.address,      "cannot prepare a space for a remote address"
        assert space.subtype == NodeSubtype.Basic, "cannot prepare a space for any subtype except basic"

        base_path = self.getBasePathFromDomain(space.domain)
        try:
            if is_empty:
                if self.fs.maybeCreateDirectory(base_path):
                    protected_name = self.settings.protectedName
                    if protected_name:
                        self.fs.maybeCreateDirectory(base_path / protected_relative_file_path_from_name(protected_name)) #TODO: get account_name!
        except OSError:
            raise SpaceUnavailable(space)
        else:
            if not (self.fs.exists(base_path) and self.fs.isdir(base_path)):
                raise SpaceUnavailable(space)

    def prepareAttachment(self, domain, (key, version, attachmentid), attachment_value, source_version):
        def change_prepared_attachments(pas):
            if pas is None:
                pas = self.makeEmptyPreparedAttachments(source_version)
            return pas.add(attachmentid)

        self.writePreparedAttachment(domain, PreparedAttachmentKey(key, version), attachmentid, attachment_value)
        prepared_attachments = self.changePreparedAttachments(domain, PreparedAttachmentKey(key, version), change_prepared_attachments)
        return prepared_attachments

    def makeEmptyPreparedAttachments(self, version):
        return PreparedRanges(version.size, self.settings.attachmentSize)

    def commit(self, domain, key, prepared_version, old_version, new_value, new_version, destination_subkeys = None, retainer_subkeys = None):
        if not (new_value is None or isinstance(new_value, FileValue)):
            raise ValueError("unrecognized FileValue: %r" % new_value)

        destination_key = self.joinCommitSubkeys(destination_subkeys)
        retainer_key    = self.joinCommitSubkeys(retainer_subkeys)

        current_version = self.getKeyedVersionFromDomainAndKey(domain, key).version
        if not old_version == current_version:
            #print ("previous version incorrect", current_version, new_version)
            if (current_version == new_version and new_value is not None and 
                self.matchesHashes(self.getFullPath(domain, key, current_version), new_value.hashes, default = False)):
                #print ("already committed", key)
                #already committed!
                #This can happen with unicode paths on OSX.  For example, u"\xed" changes to u"i\u0301"
                pass 
            else:
                raise ValueError("previous version was incorrect")
        elif new_version is None or new_value is None:
            self.moveAway(domain, key, retainer_key)
        elif prepared_version is None:
            self.setVersion(domain, key, new_version) #TOOD: do destination_key and retainer_key make sense at all here?
        elif new_version.size == 0:
            self.writeFile(domain, key, new_version, "", destination_key, retainer_key)
        else:
            self.moveFromPrepared(domain, key, prepared_version, new_value, new_version, destination_key, retainer_key)
            

        changed_keys = tuple(key for key in (key, destination_key, retainer_key) if key is not None)
        return changed_keys
        

    def joinCommitSubkeys(self, subkeys, default = None):
        if subkeys:
            first = subkeys[0]
            root, ext = os.path.splitext(first)
            joined = COMMIT_SUBKEY_JOINER.join(itertools.chain((root,), subkeys[1:])) + ext
            return joined
        else:
            return default

    def moveAway(self, domain, key, retainer_key = None):
        full_path, rel_path = self.getFullAndRelativePathsFromDomainAndKey(domain, key)
        if retainer_key:
            full_retainer_path, rel_retainer_path = self.getFullAndRelativePathsFromDomainAndKey(domain, retainer_key)
            self.moveFile(domain, full_path, full_retainer_path, rel_retainer_path)
        else:
            self.maybeMoveToTrash(domain, full_path, rel_path)
        return full_path, rel_path

    def moveFile(self, domain, from_path, to_path, rel_path = None):
        self.maybeMoveToTrash(domain, to_path, rel_path)
        self.fs.move(from_path, to_path)

    def maybeMoveToTrash(self, domain, full_path, rel_path = None):
        if rel_path is not None and self.fs.exists(full_path):
            trash_rel_path = self.getRelativePathFromDomain(domain) / rel_path #include groupname in relative trash path
            self.fs.moveToTrash(full_path, trash_rel_path)

    def setVersion(self, domain, key, version):
        full_path, rel_path = self.getFullAndRelativePathsFromDomainAndKey(domain, key)
        self.setPathVersion(full_path, version)

    def setPathVersion(self, full_path, version):
        self.fs.touch(full_path, version.mtime)

    def writeFile(self, domain, key, version, bytes, destination_key = None, retainer_key = None):
        full_path, rel_path = self.preparePath(domain, key, destination_key = destination_key, retainer_key = retainer_key)
        self.fs.write(full_path, bytes)
        self.setPathVersion(full_path, version)

    def preparePath(self, domain, key, destination_key = None, retainer_key = None):
        if destination_key:
            if retainer_key:
                self.moveAway(domain, key, retainer_key)
            return self.moveAway(domain, destination_key)
        else:
            return self.moveAway(domain, key, retainer_key)

    def moveFromPrepared(self, domain, key, prepared_version, new_value, new_version, destination_key = None, retainer_key = None):
        full_prep_path     = self.getFullPrepPathFromDomainAndKey(domain, PreparedAttachmentKey(key, prepared_version))
        full_prep_log_path = self.getFullPrepLogPathFromDomainAndKey(domain, PreparedAttachmentKey(key, prepared_version))

        try:
            if not self.fs.exists(full_prep_path):
                raise ValueError("new version is unavailable")
            elif not self.matchesHashes(full_prep_path, new_value.hashes, default = True): #TODO: warn if we have no matching hash types?
                raise CommitCorrupted("new data is corrupt")
            else:
                full_path, rel_path = self.preparePath(domain, key, destination_key = destination_key, retainer_key = retainer_key)
                #TODO: if the move below fails, we're in a hosed state where we've deleted the file.  That's REALLY BAD!
                self.fs.move(full_prep_path, full_path)
                self.setPathVersion(full_path, new_version)
        finally:
                self.maybeDeletePath(full_prep_log_path)
                self.maybeDeletePath(full_prep_path)
                self.fs.deleteEmptyParents("", self.getPrepBasePathFromDomain(domain) / key)

    def maybeDeletePath(self, full_path):
        if self.fs.exists(full_path):
            self.fs.delete(full_path)

    ### private: basic
    def getKeyedVersionsFromDomain(self, domain):
        with self.safeBasePath(domain) as base_path:
            return (self.keyedVersionFromFileSystemDetails(details) for details in self.fs.listDetails(base_path) if details is not None)

    def getKeyedVersionFromDomainAndKey(self, domain, key):
        with self.safeBasePath(domain) as base_path:
            details = self.getFileDetailsFromBasePathAndKey(base_path, key)
            version = self.versionFromFileSystemDetails(details)
            return KeyedVersion(key, version)

    def getKeyedValueFromDomainAndKey(self, domain, key):
        with self.safeBasePath(domain) as base_path:
            details = self.getFileDetailsFromBasePathAndKey(base_path, key)
            version = self.versionFromFileSystemDetails(details)
            value   = self.valueFromFileSystemDetails(details)
            return KeyedValue(key, value, version)

    def keyedVersionFromFileSystemDetails(self, details):
        path = details[1]
        return KeyedVersion(file_key_from_path(path), self.versionFromFileSystemDetails(details))

    def valueFromFileSystemDetails(self, details):
        if details is None:
            return None
        else:
            try:
                (base, path, size, mtime) = details
                full_path                 = base / path
                hashes = self.hashFile(full_path)
                return FileValue(hashes)
            except IOError:
                return None

    def versionFromFileSystemDetails(self, details):
        if details is None:
            return None
        else:
            (base, path, size, mtime) = details
            attributes = self.fs.readAttributes(base / path)

            return FileVersion(size, mtime, attributes.hidden, attributes.system)

    def getFileDetailsFromBasePathAndKey(self, base_path, key):
        path = file_key_to_path(key)
        return self.fs.getDetails(base_path, path)
        
        

    def hashFile(self, full_path, hash_types = None):
        if hash_types is None:
            hash_types = self.settings.hashTypes

        if hash_types:
            stream = self.fs.readStream(full_path)
            hashes = self.hasher.hashMulti(stream, hash_types)
            return hashes
        else:
            return ()

    def matchesHashes(self, full_path, expected_hashes, default = None):
        hash_types    = tuple(hsh.type for hsh in expected_hashes)
        try:
            actual_hashes = self.hashFile(full_path, hash_types)
        except IOError:
            actual_hashes = ()
        return self.hashesMatch(actual_hashes, expected_hashes, default = default)

    def hashesMatch(self, hashes1, hashes2, default = None):
        comparisons = tuple(hash1.bytes == hash2.bytes for hash1 in hashes1 for hash2 in hashes2 if hash1.type == hash2.type)
        if comparisons:
            return all(comparisons)
        else:
            return default

    #TODO: do mass deletion detection: readAttachmentFromFullPath might blow up
    def readAttachment(self, domain, (key, version, attachmentid)):
        return self.readAttachmentFromFullPath(self.getFullPath(domain, key, version), attachmentid)

    def getFullPath(self, domain, key, version):
        if version is not None and not version == self.getKeyedVersionFromDomainAndKey(domain, key).version:
            raise ValueError("incorrect version on file system")
        else:
            base_path = self.getBasePathFromDomain(domain)
            return base_path / file_key_to_path(key)
        

    def safeBasePath(self, domain):
        base_path = self.getBasePathFromDomain(domain)
        space     = Space.fromDomainAndAddress(domain, self.address, NodeSubtype.Basic)
        return SafeGroupBasePath(space, self.fs, base_path)

    def getBasePathFromDomain(self, domain):
        return self.getPathFromMapperAndDomain(self.groupMapper.getPath, domain)

    def getFullAndRelativePathsFromDomainAndKey(self, domain, key):
        base_path = self.getBasePathFromDomain(domain)
        rel_path  = file_key_to_path(key)
        full_path = base_path / rel_path
        return full_path, rel_path



    ### private: prepared
    def getPrepKeyedVersionsFromDomain(self, domain):
        prep_base_path = self.getPrepBasePathFromDomain(domain)
        return (self.prepKeyedVersionFromFileSystemDetails(details) for details in self.fs.listDetails(prep_base_path))

    def getPrepKeyedVersionFromDomainAndKey(self, domain, key):
        details = self.getPrepDetailsFromDomainAndKey(domain, key)
        if details is None:
            return KeyedVersion(key, None)
        else:
            return self.prepKeyedVersionFromFileSystemDetails(details)

    def prepKeyedVersionFromFileSystemDetails(self, details):
        (base, path, size, mtime) = details
        return KeyedVersion(file_prep_key_from_path(path), FilePreparedAttachmentVersion(mtime))

    def getPrepKeyedValueFromDomainAndKey(self, domain, key):
        details = self.getPrepDetailsFromDomainAndKey(domain, key)
        if details is None:
            return KeyedValue(key, None, None)
        else:
            (base, path, size, mtime) = details
            prepared_attachments      = self.readPreparedAttachmentsFromFullPath(base / path)
            if prepared_attachments is None:
                return KeyedValue(key, None, None)
            else:
                return KeyedValue(key, PreparedAttachmentValue(prepared_attachments), FilePreparedAttachmentVersion(mtime))
        
    def getPrepDetailsFromDomainAndKey(self, domain, key):
        prep_base_path = self.getPrepBasePathFromDomain(domain)
        log_path       = file_prep_key_to_log_path(key)
        return self.fs.getDetails(prep_base_path, log_path)

    def attachmentIsPrepared(self, domain, (key, version, attachmentid)):
        prepared_attachments = self.getPreparedAttachments(domain, PreparedAttachmentKey(key, version))
        return prepared_attachments is not None and prepared_attachments.contains(attachmentid)

    def getPreparedAttachments(self, domain, prep_key):
        full_prep_log_path = self.getFullPrepLogPathFromDomainAndKey(domain, prep_key)
        return self.readPreparedAttachmentsFromFullPath(full_prep_log_path)

    def readPreparedAttachmentsFromFullPath(self, full_prep_log_path):
        try:
            serialized = self.fs.read(full_prep_log_path)
            return self.deserializePreparedAttachments(serialized)
        except:
            return None


    def setPreparedAttachments(self, domain, prep_key, prepared_attachments):
        full_prep_log_path = self.getFullPrepLogPathFromDomainAndKey(domain, prep_key)
        serialized         = self.serializePreparedAttachments(prepared_attachments)
        self.fs.write(full_prep_log_path, serialized)
        return prepared_attachments

    def changePreparedAttachments(self, domain, prep_key, change):
        pas = self.getPreparedAttachments(domain, prep_key)
        new_pas = change(pas)
        return self.setPreparedAttachments(domain, prep_key, new_pas)
                                 
    def readPreparedAttachment(self, domain, attachment_key):
        if not self.attachmentIsPrepared(domain, attachment_key):
            raise ValueError("attachment isn't prepared")
        else:
            (key, version, attachmentid) = attachment_key
            full_prep_path               = self.getFullPrepPathFromDomainAndKey(domain, PreparedAttachmentKey(key, version))
            return self.readAttachmentFromFullPath(full_prep_path, attachmentid)

    def writePreparedAttachment(self, domain, (key, version), attachmentid, attachment_value):
        full_prep_path = self.getFullPrepPathFromDomainAndKey(domain, PreparedAttachmentKey(key, version))
        return self.writeAttachmentToFullPath(full_prep_path, attachmentid, attachment_value)


    def getFullPrepLogPathFromDomainAndKey(self, domain, key):
        base = self.getPrepBasePathFromDomain(domain)
        path = file_prep_key_to_log_path(key)
        return base / path

    def getFullPrepPathFromDomainAndKey(self, domain, key):
        base = self.getPrepBasePathFromDomain(domain)
        path = file_prep_key_to_path(key)
        return base / path
        
    def getRelativePathFromDomain(self, domain):
        return self.getPathFromMapperAndDomain(self.groupMapper.getRelativePath, domain)

    def getPrepBasePathFromDomain(self, domain):
        return self.getPathFromMapperAndDomain(self.groupMapper.getPrepPathForGroup, domain)

    def serializePreparedAttachments(self, prepared_attachments):
        return prepared_attachments.toBytes()

    def deserializePreparedAttachments(self, bytes):
        return PreparedRanges.fromBytes(bytes) 


    #### common
    def getPathFromMapperAndDomain(self, get_path_from_group, domain):
        #TODO: check typeid?
        (groupid, typeid) = domain

        #if typeid == self.typeid:
        try:
            return get_path_from_group(groupid)
        except GroupUnavailable:
            pass

        raise SpaceUnavailable(Space.fromDomainAndAddress(domain, self.address, NodeSubtype.Basic))
        
    def readAttachmentFromFullPath(self, full_path, attachmentid):
        rng   = attachmentid
        bytes = self.fs.read(full_path, rng)
        if len(bytes) != rng.size:
            raise ValueError("could not read correct range size from file system")
        else:
            return bytes

    def readAttachmentStreamFromFullPath(self, full_path, version):
        return self.fs.readStream(full_path, size = version.size)

    def writeAttachmentToFullPath(self, full_path, rng, bytes):
        if len(bytes) != rng.size:
            raise ValueError("could not write correct range size to file system")
        else:
            self.fs.write(full_path, bytes, start = rng.start)

    def writeAttachmentsToFullPath(self, full_path, bytes):
        self.fs.write(full_path, bytes)

class SafeGroupBasePath:
    def __init__(self,  space, fs, base_path):
        self.space    = space
        self.fs       = fs
        self.basePath = base_path

    def __enter__(self): 
        self.assertBasePathExists()
        return self.basePath

    def __exit__(self, type, error, tb):
        self.assertBasePathExists()

    def assertBasePathExists(self):
        if not self.fs.exists(self.basePath):
            raise SpaceUnavailable(self.space)


class IFilePathMapper:
    # getPath(groupid = None, key = None) -> path or GroupUnavailable
    # getPrepPath(groupid)                -> path or GroupUnavailable
    # getRelativePath(groupid)            -> path or GroupUnavailable
    pass

class FilePathMapper(IFilePathMapper):
    def __init__(self, base_path, prep_base_path, directory):
        self.basePath     = Path(base_path)
        self.prepBasePath = Path(prep_base_path)
        self.directory    = directory

    def getPath(self, groupid = None, key = None):
        relative_path = file_path_from_key(key)    if key     is not None else ""
        group_name    = self.getGroupName(groupid) if groupid is not None else ""
        return self.basePath / group_name / relative_path

    def getPrepPathForGroup(self, groupid):
        return self.prepBasePath / self.getGroupName(groupid)

    def getRelativePath(self, groupid):
        return self.getGroupName(groupid)

    def getGroupName(self, groupid):
        group = self.directory.getGroup(groupid, timeout = 0.0) #TODO: timeout?
        if group is None:
            raise GroupUnavailable(groupid)
        else:
            return group.name

class IFileNodeBrowser:
    # getLocation(groupid = None, itemid = None)
    # browse(groupid = None, itemid = None)
    pass

class FileNodeBrowser:
    def __init__(self, file_path_mapper, file_browser):
        self.filePathMapper = file_path_mapper
        self.fileBrowser    = file_browser
        
    def getLocation(self, groupid = None, key = None):
        return self.filePathMapper.getPath(groupid, key)

    def browse(self, groupid = None, key = None, browse_in_parent = None):
        location = self.getLocation(groupid, key)
        if browse_in_parent:
            location = location.parent
        return self.fileBrowser.browse(location)
    


def compile_patterns(*patterns):
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)

TEMP_FILENAME_PATTERNS   = compile_patterns(r'^~.*\.tmp$', r'^~\$', r'.+~$') #TODO: test these, r'^\.~.+\#$', r'^\#.+\#$')
SYSTEM_FILENAME_PATTERNS = compile_patterns(r'^thumbs\.db$', r'^\.DS_Store') #, r'^#.*#$')

class FileFilter(IBasicFilter):
    def __init__(self, filter_temp, filter_hidden, filter_system, filter_empty):
        self.filterTemp   = filter_temp
        self.filterHidden = filter_hidden
        self.filterSystem = filter_system
        self.filterEmpty  = filter_empty

    def keyIsFiltered(self, typeid, key):
        assert typeid == NodeType.File

        filename = key.rsplit(FILE_KEY_SEPERATOR, 1)[-1]
        return ((self.filterTemp   and self.filenameMatchesPatterns(filename, TEMP_FILENAME_PATTERNS)) or
                (self.filterSystem and self.filenameMatchesPatterns(filename, SYSTEM_FILENAME_PATTERNS)))

    def versionIsFiltered(self, typeid, key, version):
        assert typeid == NodeType.File
        assert version is not None

        return (self.filterHidden and version.hidden) or (self.filterSystem and version.system) or (self.filterEmpty and (version.size == 0))

    def filenameMatchesPatterns(self, filename, patterns):
        return any(bool(pattern.search(filename)) for pattern in patterns)

FilePriority = enum("Smallest", "Largest", "Newest", "Oldest")

class FileResolutionIntentionPrioritizer(IResolutionIntentionPrioritizer):
    def __init__(self, priority = None):
        self.priority = priority

    def calculatePriority(self, domain, key, action, address, comp_value):
        priority     = self.priority or FilePriority.Smallest
        sync_val     = comp_value.remoteValue
        file_version = sync_val.version

        if not sync_val.size:
            return HIGH_PRIORITY
        elif priority == FilePriority.Smallest:
            return file_version.size
        elif priority == FilePriority.Largest:
            return -file_version.size
        elif priority == FilePriority.Newest:
            return -file_version.mtime
        elif priority == FilePriority.Oldest:
            return file_version.mtime
        else:
            raise ValueError("Unknown file priority: %r" % (priority,))


PROTECTED_RELATIVE_FILE_PATH_PREFIX  = "From "
PROTECTED_FILE_KEY_PATTERN           = re.compile("(?:^|/)%s([^/]+)" % PROTECTED_RELATIVE_FILE_PATH_PREFIX)

def permittable_name_from_file_key(key):
    match = PROTECTED_FILE_KEY_PATTERN.search(key)
    if match:
        return match.group(1)
    else:
        return None

def protected_relative_file_path_from_name(name):
    return PROTECTED_RELATIVE_FILE_PATH_PREFIX + name
    

#TODO: make a permission checker that works for any type
class FileSyncableValuePermissionChecker(ISyncableValuePersmissionChecker):
    def __init__(self, verifier, name, directory):
        self.verifier      = verifier
        self.name          = name
        self.directory     = directory

    def makeProtectedKey(self, domain):
        assert domain.typeid == NodeType.File
        return protected_file_key_from_name(self.name)

    def checkPermission(self, domain, key, value):
        assert domain.typeid == NodeType.File

        private_name = permittable_name_from_file_key(key)
        if private_name is None:
            return SyncableValuePermission.AllowedPublic
        elif value.certificate is None:
            return SyncableValuePermission.DeniedNoCert
        else:
            author_name = self.getNameOfAuthor(value.certificate.author)
            verified    = self.verifyCertificate(key, value)
            if verified is None:
                return SyncableValuePermission.DeniedUnknownAuthor
            elif not verified:
                return SyncableValuePermission.DeniedBadCert
            elif author_name == private_name:
                return SyncableValuePermission.AllowedProtected
            elif author_name == self.name:
                return SyncableValuePermission.AllowedPrivate
            else:
                return SyncableValuePermission.DeniedWrongAuthor

    def verifyCertificate(self, key, value):
        try:
            return self.verifier.verify((key, value.setCertificate(None)), value.certificate, self.getPublicKeyOfAuthor)
        except: #TODO: Timeout? return False?
            return False
        
    def getNameOfAuthor(self, peerid):
        peer = self.directory.getPeer(peerid, timeout = 30.0) #TODO: timeout?
        if peer is None:
            return None
        else:
            return peer.accountName

    def getPublicKeyOfAuthor(self, peerid):
        peer = self.directory.getPeer(peerid, timeout = 30.0) #TODO: timeout?
        if peer is None:
            return None
        else:
            return crypto.deserialize_public_key(peer.publicKey)

