# Copyright (c) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""
Locking on a file implemented via Unix PID files. Based on
https://github.com/smontanaro/pylockfile/blob/master/lockfile/pidlockfile.py
"""
from __future__ import absolute_import

import os
import errno
import socket
import time


class LockError(Exception):
    """Base class for error arising from attempts to acquire the lock."""
    pass


class LockTimeout(LockError):
    """Raised when lock creation fails within a user-defined period of time."""
    pass


class AlreadyLocked(LockError):
    """Some other thread/process is locking the file."""
    pass


class LockFailed(LockError):
    """Lock file creation failed for some other reason."""
    pass


class UnlockError(Exception):
    """Base class for errors arising from attempts to release the lock."""
    pass


class NotLocked(UnlockError):
    """Raised when an attempt is made to unlock an unlocked file."""
    pass


class NotMyLock(UnlockError):
    """Raised when an attempt is made to unlock a file someone else locked."""
    pass


class PIDLockFile(object):
    """
    Locking implemented as a Unix PID file.

    The lock file is a normal file named by the attribute ``path``. A lock's PID file contains a single line of text,
    containing the process ID (PID) of the process that acquired the lock.

    **WARNING** This is **NOT** a reentrant lock!
    """
    def __init__(self, path):
        """
        Create a new lock of file at ``path`` and default timeout of ``timeout``.
        """
        self.path = os.path.abspath(path) + ".lock"
        self.lock_file = os.path.abspath(path) + ".lock"
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.unique_name = self.path

    def read_pid(self):
        """Get the PID from the lock file."""
        return PIDLockFile._read_pid_from_pidfile(self.path)

    def is_locked(self):
        """
        Test if the lock is currently held.

        The lock is held if the PID file for this lock exists.
        """
        return os.path.exists(self.path)

    def i_am_locking(self):
        """
        Test if the lock is held by the current process.

        Returns ``True`` if the current process ID matches the number stored in the PID file.
        """
        return self.is_locked() and os.getpid() == self.read_pid()

    def acquire(self, timeout=None):
        """
        Acquire the lock.

        Creates the PID file for this lock, or raises an error if the lock could not be acquired.

        If ``timeout`` is omitted (or None), wait forever trying to lock the file.

        If ``timeout`` > 0, try to acquire the lock for that many seconds. If the lock period expires and the file is
        still locked, raise ``LockTimeout``.

        If ``timeout`` <= 0, raise AlreadyLocked immediately if the file is already locked.
        """
        end_time = time.time()
        if timeout is not None and timeout > 0:
            end_time += timeout

        while True:
            try:
                PIDLockFile._write_pid_to_pidfile(self.path)
            except OSError as exc:
                if exc.errno == errno.EEXIST:  # Failed to create lock file because it exists (already locked)
                    if timeout is not None and time.time() > end_time:
                        if timeout > 0:
                            raise LockTimeout("Timeout waiting to acquire lock for {}".format(self.path))
                        else:
                            raise AlreadyLocked("{} is already locked".format(self.path))
                    time.sleep(timeout is not None and timeout/10 or 0.1)
                else:
                    raise LockFailed("failed to create {} - errno:{}".format(self.path, exc.errno))
            else:
                return

    def release(self):
        """
        Release the lock.

        Removes the PID file to release the lock, or raises an error if the current process does not hold the lock.

        """
        if not self.is_locked():
            raise NotLocked("{} is not locked".format(self.path))
        if not self.i_am_locking():
            raise NotMyLock("{} is locked, but not by me".format(self.path))
        PIDLockFile._remove_existing_pidfile(self.path)

    @staticmethod
    def _read_pid_from_pidfile(pidfile_path):
        """
        Read the PID recorded in the named PID file.

        Read and return the numeric PID recorded as text in the named PID file. If the PID file cannot be read, or if
        the content is not a valid PID, return ``None``.

        """
        pid = None
        try:
            pidfile = open(pidfile_path, 'r')
        except IOError:
            pass
        else:
            # According to the FHS 2.3 section on PID files in /var/run:
            #
            #   The file must consist of the process identifier in ASCII-encoded decimal, followed by a newline
            #   character.
            #
            #   Programs that read PID files should be somewhat flexible in what they accept; i.e., they should ignore
            #   extra whitespace, leading zeroes, absence of the trailing newline, or additional lines in the PID file.
            line = pidfile.readline().strip()
            try:
                pid = int(line)
            except ValueError:
                pass
            pidfile.close()
        return pid

    @staticmethod
    def _write_pid_to_pidfile(pidfile_path):
        """Write the current pid to the pidfile at ``pidfile_path``."""
        open_flags = (os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        open_mode = 0o644
        pidfile_fd = os.open(pidfile_path, open_flags, open_mode)
        pidfile = os.fdopen(pidfile_fd, 'w')
        # According to the FHS 2.3 section on PID files in /var/run:
        #
        #   The file must consist of the process identifier in ASCII-encoded decimal, followed by a newline character.
        #   For example, if crond was process number 25, /var/run/crond.pid would contain three characters: two, five,
        #   and newline.
        pid = os.getpid()
        line = "{:d}\n".format(pid)
        pidfile.write(line)
        pidfile.close()

    @staticmethod
    def _remove_existing_pidfile(pidfile_path):
        """
        Remove the named PID file if it exists.

        Removing a PID file that doesn't already exist puts us in the desired state, so we ignore the condition if the
        file does not exist.

        """
        try:
            os.remove(pidfile_path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            else:
                raise
