# Copyright (C) 2012-2014 Kapiche Limited
# Author: Ryan Stuart <ryan@kapiche.com>
"""Tests for caterpillar/locking.py."""
import os
import errno
import pytest

from caterpillar.locking import PIDLockFile, AlreadyLocked, LockFailed, NotLocked, NotMyLock


def test_lock_acquire(index_dir):
    os.makedirs(index_dir)
    lock = PIDLockFile(os.path.join(index_dir, 'writer'))
    assert not lock.is_locked()
    assert not lock.i_am_locking()
    assert lock.read_pid() is None

    lock.acquire()
    assert lock.is_locked()
    assert lock.i_am_locking()
    assert lock.read_pid() == os.getpid()


def test_lock_failed(index_dir):
    lock = PIDLockFile(os.path.join(index_dir, 'writer'))
    with pytest.raises(LockFailed):
        lock.acquire()

    os.makedirs(index_dir)
    lock1 = PIDLockFile(os.path.join(index_dir, 'writer'))
    lock2 = PIDLockFile(os.path.join(index_dir, 'writer'))
    lock1.acquire()

    with pytest.raises(AlreadyLocked):
        lock2.acquire(timeout=0)


def test_lock_release(index_dir, monkeypatch):
    """Test the lock release functionality."""
    os.makedirs(index_dir)
    lock1 = PIDLockFile(os.path.join(index_dir, 'writer'))
    lock2 = PIDLockFile(os.path.join(index_dir, 'writer'))

    with pytest.raises(NotLocked):
        lock1.release()

    lock1.acquire()
    lock1.release()
    assert not lock1.is_locked()

    lock1.acquire()
    monkeypatch.setattr(os, "getpid", lambda: 1)  # Modify os.getpid() to get the error we want.
    with pytest.raises(NotMyLock):
        lock2.release()


def test_locking_interals(index_dir, monkeypatch):
    """Test error conditions of various private methods."""
    os.makedirs(index_dir)
    lock = PIDLockFile(os.path.join(index_dir, "writer"))

    lock.acquire()
    with open(lock.path, 'w') as lock_file:
        lock_file.write('xxx')
    assert lock.read_pid() is None
    with open(lock.path, 'w') as lock_file:
        lock_file.write("{}".format(os.getpid()))

    def error_raiser(code):
        raise OSError(code, "A message")

    monkeypatch.setattr(os, "remove", lambda x: error_raiser(errno.ENOENT))  # Simulate file not existing
    lock.release()

    monkeypatch.setattr(os, "remove", lambda x: error_raiser(errno.EACCES))  # Simulate other error
    with pytest.raises(OSError):
        lock.release()
