# Copyright (c) 2017 Kapiche Limited
# Author: Sam Hames <sam.hames@kapiche.com>
"""
Logic for migrating data through different versions of :class:`.SqliteStorage`.

Each distinct schema version of an index is represented by an integer: the newest schema
version should be one more than the next newest schema version and so on.

A single migration is represented by a class that implements :staticmethod:`up` and :staticmethod:`down` as well as
having attributes :attribute:`from_schema_version` and :attribute:`to_schema_version`. The 'up' migration represents the
work to migrate a data at schema version :attribute:`from_schema_version` to version :attribute:`to_schema_version`,
while the 'down' migration represents the reverse. Each migration should be self contained, and take place inside a
transaction, so that the SQLiteWriter can rollback to the initial point if there are any errors.

.. warning: Individual migrations do not check the writer is at a valid version to apply the migration. Use the
    :method:`migrate` of :class:SqliteWriter to manage migrations. Applying a migration outside this could lead to an
    index being left in an unreadable or inconsistent state.

The order that migrations are applied is specifically defined by the list ``MIGRATIONS``. Currently the migration logic
can only correctly handle linear application of consecutive migrations in the order defined in this list. In the future
this may be extended to allow fast paths for migrating between specific versions,

"""

import abc

from ._sqlite_v0_10_0_schema import v0_10_0_schema


class Migration(object):
    """ """
    __metaclass__ = abc.ABCMeta

    @staticmethod
    @abc.abstractmethod
    def up():
        """Migrate a database from `from_schema_version` to `to_schema_version`."""
        return

    @staticmethod
    @abc.abstractmethod
    def down():
        """Migrate a database from `to_schema_version` back to `from_schema_version`."""
        return


class InitialiseSchema(Migration):
    """
    Setup the database with the initial schema, including the migrations table.

    This will also migrate databases created with caterpillar versions between
    0.10.0 and the introduction of the migration logic to a known fixed point.

    Also sets up the necessary database file options that will persist through
    all schema versions (WAL and pagesize).

    """
    from_schema_version = None
    to_schema_version = 0

    @staticmethod
    def up(writer):
        cursor = writer._db_connection.cursor()
        list(cursor.execute(v0_10_0_schema))

    @staticmethod
    def down(writer):
        cursor = writer._db_connection.cursor()
        cursor.execute('drop table migrations')


# This is the control scheme for the order of migration operations.
MIGRATIONS = [
    InitialiseSchema
]
