#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

import os
import hashlib

class FileHandle(object):
    def __init__(self, name, fullpath, mode, fid):
        self.fullpath = fullpath
        self.name = name
        self.mode = mode
        self.fid = fid
        self.offset = 0
        self.size = 0
        self.access = False
        self.uid = 0
        self.gid = 0
        self.times = (0, 0)
        self.flags = os.O_RDONLY # default
        self.is_loaded = False

    def load(self):
        print "WTF..."
        pass

    def unload(self):
        print "%s unloaded" % (self.fullpath)
        self.data = None
        self.size = 0
        self.is_loaded = False

    def read(self, offset, length):
        print "READ %s %d %d %d %s" % (self.fullpath, offset, length, self.size, str(self.data))
        max_offset = max(self.size, offset + length)
        if offset > self.size:
            return None
        else:
            return self.data[offset:max_offset]

    def write(self, buff, offset):
        length = len(buff) + offset
        if length > self.size:
            self.size = length
        self.data = buff
        return len(buff)

    def truncate(self, length):
        ''' Truncate the file to specified length.
        '''
        self.size = length
        self.data = self.data[:length]

    def fsync(self):
        ''' Force a write to the file system.
        '''
        print "FSYNC called to %s" % (self.fullpath)
        if self.data != None:
            with open(self.fullpath, "w") as fh:
                hasher = hashlib.new("sha256")
                hasher.update(self.data)
                digest = hasher.hexdigest()
                fh.write(digest)
                self.data = digest
                self.size = len(digest)

                # fh.write(self.data)

    def close(self):
        ''' Unload and release all resources.
        '''
        self.unload()
        print "STill loaded? %d" % (self.is_loaded)

class LocalFileHandle(FileHandle):
    def __init__(self, name, fullpath, mode, fid):
        super(LocalFileHandle, self).__init__(name, fullpath, mode, fid)
        result = os.open(fullpath, os.O_RDWR | os.O_CREAT, mode)
        print "OS open result %d" % (result)
        self.load()

    def load(self):
        print "trying to load... %d" % (self.is_loaded)
        if not self.is_loaded:
            print "LOCAL LOAD %s" % (self.fullpath)
            # TODO: we'd do the decryption here
            with open(self.fullpath) as fhandle:
                self.data = fhandle.read()
                self.size = len(self.data)
            self.is_loaded = True
            print "lOADED!"
        return self

    def __str__(self):
        return self.fullpath + "-" + str(self.fid)

# class RemoteFileHandle(FileHandle):
#     def __init__(self, name, fullpath, fid, client):
#         super(LocalFileHandle, self).__init__(name, fullpath, 0, fid)
#         self.client = client
#
#     def load(self):
#         print "REMOTE LOAD %s" % (self.fullpath)
#         self.data, self.expiry = self.client.volatile_get(name)
#         if data != None:
#             self.size = len(data)
#
#             # Start a timer to refresh the content when the expiration time is up
#             timer = Timer(self.expiry, self.load)
#             timer.start()
#         return self
