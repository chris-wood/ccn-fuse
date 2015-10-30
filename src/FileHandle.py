#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

from CCNxClient import *
from threading import Timer

class FileHandle(Object):
    def __init__(self, name, fullpath, mode, fid):
        self.fullpath = fullpath
        self.name = name
        self.mode = mode
        self.fid = fid
        self.offset = 0
        self.size = 0
        self.access = False
        self.mode = stat.S_READ
        self.uid = 0
        self.gid = 0
        self.times = (0, 0)
        self.flags = os.O_RDONLY # default

    def load(self):
        pass

    def unload(self):
        self.data = None

    def read(self):
        max_offset = max(self.size - 1, offset + length)
        if offset >= self.size:
            return None
        else:
            return self.data[offset:max_offset]

    def write(self, buff, offset):
        length = len(buff) + offset
        if length > self.size:
            self.size = length
        self.data[offset:length] = buff

    def truncate(self, length):
        ''' Truncate the file to specified length.
        '''
        self.size = length

    def fsync(self):
        ''' Force a write to the file system.
        '''
        if self.data != None:
            with open(self.fullpath, "w") as fhandle:
                fhandle.write(self.data)

    def close(self):
        ''' Unload and release all resources.
        '''
        self.unload()
        self.data = None

class LocalFileHandle(FileHandle):
    def __init__(self, name, fullpath, mode, fid):
        super(LocalFileHandle, self).__init__(name, fullpath, mode, fid)

    def load(self):
        with open(self.name) as fhandle:
            self.data = fhandle.read()
            self.size = len(data)
        return self

class RemoteFileHandle(FileHandle):
    def __init__(self, name, fullpath, fid, client):
        super(LocalFileHandle, self).__init__(name, fullpath, 0, fid)
        self.client = client

    def load(self):
        self.data, self.expiry = self.client.volatile_get(name)
        if data != None:
            self.size = len(data)

            # Start a timer to refresh the content when the expiration time is up
            timer = Timer(self.expiry, self.load)
            timer.start()
        return self
