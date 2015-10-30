#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

# DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
# Copyright 2015 Palo Alto Research Center, Inc. (PARC), a Xerox company.  All Rights Reserved.
# The content of this file, whole or in part, is subject to licensing terms.
# If distributing this software, include this License Header Notice in each
# file and provide the accompanying LICENSE file.

# @author Christopher A. Wood, System Sciences Laboratory, PARC
# @copyright 2015 Palo Alto Research Center, Inc. (PARC), A Xerox Company. All Rights Reserved.

from __future__ import with_statement

import os
import sys
import errno
import argparse
import time
import tempfile
import json
import stat

from threading import Timer
from fuse import FUSE, FuseOSError, Operations

sys.path.append('/Users/cwood/PARC/Distillery/build/lib/python2.7/site-packages')
from CCNx import *

class CCNxClient(object):
    def __init__(self, async = False):
        self.portal = self.openAsyncPortal() if async else self.openPortal()

    def setupIdentity(self):
        global IDENTITY_FILE
        IDENTITY_FILE = tempfile.NamedTemporaryFile(suffix=".p12")
        identity = create_pkcs12_keystore(IDENTITY_FILE.name, "foobar", "bletch", 1024, 10)
        return identity

    def openPortal(self):
        identity = self.setupIdentity()
        factory = PortalFactory(identity)
        portal = factory.create_portal()
        return portal

    def openAsyncPortal(self):
        identity = self.setupIdentity()
        factory = PortalFactory(identity)
        portal = factory.create_portal(transport=TransportType_RTA_Message, \
            attributes=PortalAttributes_NonBlocking)
        return portal

    def get(self, name, data):
        interest = Interest(Name(name))
        if data != None:
            interest.setPayload(data)

        self.portal.send(interest)
        response = self.portal.receive()

        if isinstance(response, ContentObject):
            return response.getPayload()
        else:
            return None

    def get_async(self, name, data, timeout_seconds):
        interest = None
        if data == None:
            interest = Interest(Name(name))
        else:
            interest = Interest(Name(name), payload=data)

        for i in range(timeout_seconds):
            try:
                self.portal.send(interest)
                response = self.portal.receive()
                if response and isinstance(response, ContentObject):
                    return response.getPayload()
            except Portal.CommunicationsError as x:
                if x.errno == errno.EAGAIN:
                    time.sleep(1)
                else:
                    raise
        return None

    def push(self, name, data):
        interest = Interest(Name(name), payload=data)
        try:
            self.portal.send(interest)
        except Portal.CommunicationsError as x:
            sys.stderr.write("ccnxPortal_Write failed: %d\n" % (x.errno,))
        pass

    def listen(self, prefix):
        try:
            self.portal.listen(Name(prefix))
        except Portal.CommunicationsError as x:
            sys.stderr.write("CCNxClient: comm error attempting to listen: %s\n" % (x.errno,))
        return True

    def receive(self):
        request = self.portal.receive()
        if isinstance(request, Interest):
            return str(request.name), request.getPayload()
        else:
            pass
        return None, None

    def receive_raw(self):
        request = self.portal.receive()
        if isinstance(request, Interest):
            return request.name, request.getPayload()
        else:
            pass
        return None, None

    def reply(self, name, data):
        try:
            self.portal.send(ContentObject(Name(name), data))
        except Portal.CommunicationsError as x:
            sys.stderr.write("reply failed: %d\n" % (x.errno,))

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

class ContentStore(Object):
    def __init__(self, root):
        self.root = root
        self.files = {}
        self.handles = {}
        self.descriptor_seq = 0

    def contains_file(self, name):
        return name in self.files

    def load(self, name):
        return self.files[name].load()

    def open(self, name, flags):
        if self.contains_file(name):
            self.files[name].flags = flags
            return self.load(name).fid
        else:
            return self.create_remote_file(name).load().fid

    def get_handle_from_path(self, path):
        if path in self.files:
            return self.files[path]
        else:
            raise Exception("File %s does not exist" % (path))

    def get_handle(self, fh):
        if fh in self.handles:
            return self.handles[fh]
        else:
            raise Exception("File handle %d does not exist" % (fh))

    def create_local_file(self, name, mode):
        if name not in self.files:
            self.files[name] = LocalFileHandle(name, os.path.join(self.root, name), mode, descriptor_seq)
            self.handles[descriptor_seq] = self.files[name]
            descriptor_seq += 1
        return self.files[name]

    def create_remote_file(self, name, mode):
        if name not in self.files:
            self.files[name] = RemoteFileHandle(name, os.path.join(self.root, name), descriptor_seq)
            self.handles[descriptor_seq] = self.files[name]
            descriptor_seq += 1
        return self.files[name]

    def get_files_in_namespace(files, prefix):
        fileset = []
        for fhandle in self.files:
            if fhandle.name.startswith(prefix):
                fileset.append(fhandle.name)
        return fileset

    def read_namespace(self, prefix):
        return get_files_in_namespace(self.files, prefix)

    def delete_namespace(self, prefix):
        fileset = get_files_in_namespace(self.files, prefix)
        for fhandle in fileset:
            fhandle.unload()
            self.files.pop(fhandle.name, None)

    def access(self, name):
        return self.files[name].access()

    def chmod(self, name, mode):
        return self.files[name].mode = mode

    def chown(self, name, uid, gid):
        self.files[name].uid = uid
        self.files[name].uid = gid
        return True

    def symlink(self, name, target):
        if name not in self.files:
            raise Exception("%s not a valid file" % (name))
        if target in self.files:
            return
        self.files[target] = self.files[name]

    def unlink(self, name):
        if name not in self.files:
            raise Exception("%s not a valid file" % (name))
        self.files[name].close()
        del self.files[name]

    def utime(self, name, times):
        if name not in self.files:
            raise Exception("%s not a valid file" % (name))
        self.files[name].times = times

class CCNxDrive(Operations):
    def __init__(self, root):
        self.root = root
        self.client = CCNxClient()
        self.content_store = ContentStore(root)

    def access(self, path, mode):
        ''' Return True if access is allowed, and False otherwise.
        '''
        return self.content_store.access(path)

    def chmod(self, path, mode):
        ''' ???
        '''
        return self.content_store.chmod(path, mode)

    def chown(self, path, uid, gid):
        ''' ???
        '''
        return self.content_store.chown(path, uid, gid)

    def getattr(self, path, fh=None):
        return {}

    def readdir(self, path, fh):
        return self.content_store.read_namespace(path)

    def readlink(self, path):
        ''' Return a string representing the path to which the symbolic link points.
        Names are names in CCN, so we just return the path.
        '''
        return path

    def mknod(self, path, mode, dev):
        # return os.mknod(self._full_path(path), mode, dev)
        # TODO: is this the same as a Manifest?
        raise Exception()

    def rmdir(self, path):
        return self.content_store.delete_namespace(path)

    def mkdir(self, path, mode):
        # return os.mkdir(self._full_path(path), mode)
        # TODO: is this the same as a Manifest?
        raise Exception()

    def statfs(self, path):
        # stv = os.statvfs(path)
        # return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
        #     'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
        #     'f_frsize', 'f_namemax'))
        raise Exception("statfs not implemented.")

    def unlink(self, path):
        self.content_store.unlink(path)

    def symlink(self, name, target):
        self.content_store.symlink(name, target)

    def rename(self, old, new):
        raise Exception("rename not implemented")

    def link(self, target, name):
        # return os.link(target, name)
        raise Exception("link not implemented")

    def utimens(self, path, times=None):
        self.content_store.utime(path, times)

    def open(self, path, flags):
        return self.content_store.open(path, flags)

    def create(self, path, mode, fi=None):
        return self.content_store.create_local_file(path, mode).fid

    def read(self, path, length, offset, fh = None):
        handle = self.content_store.get_handle_from_path(path)
        return handle.read(length, offset)

    def write(self, path, buffer, offset, fh = None):
        handle = self.content_store.get_handle_from_path(path)
        return handle.write(buffer, offset)

    def truncate(self, path, length, fh = None):
        handle = self.content_store.get_handle_from_path(path)
        return handle.truncate(length)

    def flush(self, path, fh):
        handle = self.content_store.get_handle_from_path(path)
        return handle.fsync(length)

    def release(self, path, fh):
        handle = self.content_store.get_handle_from_path(path)
        return handle.close(length)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

def main(mountpoint, root):
    FUSE(CCNxDrive(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    desc = '''CCN-FUSE: The FUSE adapter for CCN.
    '''

    parser = argparse.ArgumentParser(prog='ccn-fuse', formatter_class=argparse.RawDescriptionHelpFormatter, description=desc)
    parser.add_argument('-m', '--mount', action="store", required=True, help="The CCN-FUSE moint point.")
    parser.add_argument('-r', '--root', action="store", required=True, help="The root of the CCN-FUSE file system.")

    args = parser.parse_args()

    main(args.mount, args.root)
