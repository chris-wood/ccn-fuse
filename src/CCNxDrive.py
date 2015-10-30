#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

from __future__ import with_statement

import os
import sys
import errno
import argparse
import time
import tempfile
import json
import stat

from CCNxClient import *
from fuse import FUSE, FuseOSError, Operations

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
