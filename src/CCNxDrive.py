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
from ContentStore import *
from FileHandle import *
from fuse import FUSE, FuseOSError, Operations

def display_args(func):
    argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
    fname = func.func_name
    def echo_func(*args,**kwargs):
        print fname, "(", ', '.join(
            '%s=%r' % entry
            for entry in zip(argnames,args[:len(argnames)]) + [("args",list(args[len(argnames):]))] + [("kwargs",kwargs)]) + " )"
        print >> sys.stdout, fname, "(", ', '.join(
            '%s=%r' % entry
            for entry in zip(argnames,args[:len(argnames)]) + [("args",list(args[len(argnames):]))] + [("kwargs",kwargs)]) + " )"
    return func

class CCNxDrive(Operations):
    @display_args
    def __init__(self, root):
        self.root = root
        self.content_store = ContentStore(root)

    @display_args
    def access(self, path, mode):
        ''' Return True if access is allowed, and False otherwise.
        '''
        return self.content_store.access(path)

    @display_args
    def chmod(self, path, mode):
        ''' ???
        '''
        return self.content_store.chmod(path, mode)

    @display_args
    def chown(self, path, uid, gid):
        ''' ???
        '''
        return self.content_store.chown(path, uid, gid)

    @display_args
    def getattr(self, path, fh=None):
        return {}

    @display_args
    def readdir(self, path, fh):
        return self.content_store.read_namespace(path)

    @display_args
    def readlink(self, path):
        ''' Return a string representing the path to which the symbolic link points.
        Names are names in CCN, so we just return the path.
        '''
        return path

    @display_args
    def mknod(self, path, mode, dev):
        # return os.mknod(self._full_path(path), mode, dev)
        # TODO: is this the same as a Manifest?
        raise Exception()

    @display_args
    def rmdir(self, path):
        return self.content_store.delete_namespace(path)

    def mkdir(self, path, mode):
        # return os.mkdir(self._full_path(path), mode)
        # TODO: is this the same as a Manifest?
        raise Exception()

    @display_args
    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))
        # raise Exception("statfs not implemented.")

    @display_args
    def unlink(self, path):
        self.content_store.unlink(path)

    @display_args
    def symlink(self, name, target):
        self.content_store.symlink(name, target)

    @display_args
    def rename(self, old, new):
        # TODO: do it
        pass
        # raise Exception("rename not implemented")

    @display_args
    def link(self, target, name):
        # TODO: do it
        return os.link(target, name)
        # raise Exception("link not implemented")

    @display_args
    def utimens(self, path, times=None):
        self.content_store.utime(path, times)

    @display_args
    def open(self, path, flags):
        return self.content_store.open(path, flags)

    @display_args
    def create(self, path, mode, fi=None):
        return self.content_store.create_local_file(path, mode).fid

    @display_args
    def read(self, path, length, offset, fh = None):
        handle = self.content_store.get_handle_from_path(path)
        return handle.read(length, offset)

    @display_args
    def write(self, path, buffer, offset, fh = None):
        handle = self.content_store.get_handle_from_path(path)
        return handle.write(buffer, offset)

    @display_args
    def truncate(self, path, length, fh = None):
        handle = self.content_store.get_handle_from_path(path)
        return handle.truncate(length)

    @display_args
    def flush(self, path, fh):
        handle = self.content_store.get_handle_from_path(path)
        return handle.fsync(length)

    @display_args
    def release(self, path, fh):
        handle = self.content_store.get_handle_from_path(path)
        return handle.close(length)

    @display_args
    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

@display_args
def main(mountpoint, root):
    drive = CCNxDrive(root)
    FUSE(drive, mountpoint, nothreads=True, foreground=True) # run until done.

if __name__ == '__main__':
    desc = '''CCN-FUSE: The FUSE adapter for CCN.
    '''

    parser = argparse.ArgumentParser(prog='ccn-fuse', formatter_class=argparse.RawDescriptionHelpFormatter, description=desc)
    parser.add_argument('-m', '--mount', action="store", required=True, help="The CCN-FUSE moint point.")
    parser.add_argument('-r', '--root', action="store", required=True, help="The root of the CCN-FUSE file system.")

    args = parser.parse_args()

    main(args.mount, args.root)
