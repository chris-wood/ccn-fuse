from __future__ import with_statement

import os
import sys
import errno
from ContentStore import *

from fuse import FUSE, FuseOSError, Operations

class FileSystemFacade(Operations):
    def __init__(self, root):
        self.root = root
        self.content_store = ContentStore(root)

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    def open(self, path, flags):
        print "open"
        # full_path = self._full_path(path)
        # fid = os.open(full_path, flags)

        handle = self.content_store.open(path, flags)

        return handle

    def create(self, path, mode, fi=None):
        print "create"

        # full_path = self._full_path(path)
        # fid = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
        handle = self.content_store.create_local_file(path, mode)

        return handle.fid

    def read(self, path, length, offset, fh):
        print "read %s" % (path)
        handle = self.content_store.get_handle(fh)
        return handle.read(offset, length)
        # os.lseek(fh, offset, os.SEEK_SET)
        # return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        print "write"
        # os.lseek(fh, offset, os.SEEK_SET)
        # return os.write(fh, buf)
        handle = self.content_store.get_handle(fh)
        return handle.write(buf, offset)

    def truncate(self, path, length, fh=None):
        print "truncate"
        # full_path = self._full_path(path)
        # with open(full_path, 'r+') as f:
        #     f.truncate(length)

    def flush(self, path, fh):
        if not self.content_store.contains_file(path):
            self.content_store.create_local_file(path, os.O_CREAT | os.O_RDWR)
        print "flushing %s" % (path)
        # return os.fsync(fh)
        self.content_store.fsync(fh)

    def release(self, path, fh):
        # return os.close(fh)
        print "RELEASE AND CLOSE!"
        handle = self.content_store.get_handle(fh)
        return handle.close()

    def fsync(self, path, fdatasync, fh):
        print "fsyncing"
        return self.flush(path, fh)

def main(mountpoint, root):
    # The file system is rooted at $(root), where they are modified and whatnot
    # The files in the directory can be used from the mount point
    FUSE(FileSystemFacade(root), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
