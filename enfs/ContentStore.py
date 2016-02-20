#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

import os
import os.path
import sys
import errno
import argparse
# import time
import tempfile
import json
import stat

from FileHandle import *

class ContentStore(object):
    def __init__(self, root):
        self.root = root
        self.files = {} # root is always in there...
        self.handles = {}
        self.descriptor_seq = 0

    def _full_path(self, path):
        if path.startswith("/"):
            path = path[1:]
        path = os.path.join(self.root, path)
        return path

    def contains_handle(self, fid):
        return fid in self.handles

    def contains_file(self, name):
        return name in self.files

    def load(self, name):
        return self.files[name].load()

    def open(self, name, flags):
        print "CS OPEN %s" % (name)
        fullpath = self._full_path(name)
        if name in self.files:
            print "loading old copy"
            return self.files[name].load().fid
        elif os.path.isfile(fullpath):
            access_mode = os.R_OK | os.W_OK | os.X_OK
            return self.create_local_file(name, access_mode).load().fid
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
        fullpath = self._full_path(name)
        print "creating a local file %d" % (self.descriptor_seq)
        if not self.contains_file(name):
            self.files[name] = LocalFileHandle(name, fullpath, mode, self.descriptor_seq)
            self.handles[self.descriptor_seq] = self.files[name]
            self.descriptor_seq += 1
        return self.files[name]

    def fsync(self, fid):
        print "fsync in the ContentStore: %s " % (str(fid))
        for handle in self.handles:
            print str(handle)
        handle = self.handles[fid]
        handle.fsync()

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
        if not self.contains_file(name):
            print "need to fetch it... and if not here, raise Exception"

    def chmod(self, name, mode):
        self.files[name].mode = mode
        return mode

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
