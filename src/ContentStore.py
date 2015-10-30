#!/usr/bin/python

# -*- mode: python; tab-width: 4; indent-tabs-mode: nil -*-

import os
import sys
import errno
import argparse
import time
import tempfile
import json
import stat

from FileHandle import *
from CCNxClient import *

class ContentStore(object):
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
