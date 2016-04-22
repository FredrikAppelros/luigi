# -*- coding: utf-8 -*-
"""
Implements a subclass of :py:class:`~luigi.target.Target` that writes data to Vertica.
"""

import datetime
import logging
import tempfile

import luigi

from luigi import six

from . import rdbms

logger = logging.getLogger('luigi-interface')

try:
    import vertica_python
    import psycopg2
    assert vertica_python
    assert psycopg2
except ImportError:
    logger.warning("Loading vertica module without vertica-python and psycopg2 installed. Will crash at runtime if vertica functionality is used.")


class VerticaTarget(luigi.Target):
    """
    Target for a resource in Vertica.

    This will rarely have to be directly instantiated by the user.
    """
    marker_table = luigi.configuration.get_config().get('vertica', 'marker-table', 'table_updates')

    # Use DB side timestamps or client side timestamps in the marker_table
    use_db_timestamps = True

    def __init__(
        self, host, database, user, password, table, update_id, port=None
    ):
        """
        Args:
            host (str): Vertica server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set
            port (int): Vertica server port.

        """
        if ':' in host:
            self.host, self.port = host.split(':')
        else:
            self.host = host
            self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

    def touch(self, connection=None):
        """
        Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()

        if self.use_db_timestamps:
            connection.cursor().execute(
                "INSERT INTO {} (update_id, target_table) "
                "VALUES (:uid, :table)"
                .format(self.marker_table),
                {
                    'uid': self.update_id,
                    'table': self.table
                }
            )
        else:
            connection.cursor().execute(
                "INSERT INTO {} (update_id, target_table, inserted) "
                "VALUES (:uid, :table, :time)"
                .format(self.marker_table),
                {
                    'uid': self.update_id,
                    'table': self.table,
                    'time': datetime.datetime.now()
                }
            )

        connection.commit()

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT 1 FROM {} "
                "WHERE update_id = :uid "
                "LIMIT 1"
                .format(self.marker_table),
                {'uid': self.update_id}
            )
            row = cursor.fetchone()
        except vertica_python.errors.MissingRelation:
            row = None
        return row is not None

    def connect(self):
        """
        Get a vertica_python connection object to the database where the table is.
        """
        connection = vertica_python.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password)
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        with self.connect() as connection:
            cursor = connection.cursor()
            if self.use_db_timestamps:
                sql = (
                    "CREATE TABLE {} ("
                    "update_id VARCHAR PRIMARY KEY, "
                    "target_table VARCHAR, "
                    "inserted TIMESTAMP DEFAULT NOW()"
                    ")"
                ).format(self.marker_table)
            else:
                sql = (
                    "CREATE TABLE {} ("
                    "update_id VARCHAR PRIMARY KEY, "
                    "target_table VARCHAR, "
                    "inserted TIMESTAMP"
                    ")"
                ).format(self.marker_table)
            try:
                cursor.execute(sql)
            except vertica_python.errors.DuplicateObject:
                pass

    def open(self, mode):
        raise NotImplementedError("Cannot open() VerticaTarget")


class CopyToTable(rdbms.CopyToTable):
    """
    Template task for inserting a data set into Vertica

    Usage:
    Subclass and override the required `host`, `database`, `user`,
    `password`, `table` and `columns` attributes.

    To customize how to access data from an input task, override the `rows` method
    with a generator that yields each row as a tuple with fields ordered according to `columns`.
    """

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

    def map_column(self, value):
        """
        Applied to each column of every row returned by `rows`.

        Default behaviour is to escape special characters and identify any self.null_values.
        """
        if value in self.null_values:
            return ''
        else:
            return six.text_type(value)

    def output(self):
        """
        Returns a VerticaTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return VerticaTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )

    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))
        cursor.copy(
            "COPY {} ({}) FROM STDIN DELIMITER '{}' ".format(
                self.table, ','.join(column_names), self.column_separator
            ),
            file
        )

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        tmp_dir = luigi.configuration.get_config().get('vertica', 'local-tmp-dir', None)

        with tempfile.TemporaryFile(dir=tmp_dir) as tmp_file, self.output().connect() as connection:
            # transform all data generated by rows() using map_column and write data
            # to a temporary file for import using COPY
            n = 0
            for row in self.rows():
                n += 1
                if n % 100000 == 0:
                    logger.info("Wrote %d lines", n)
                rowstr = self.column_separator.join(self.map_column(val) for val in row)
                rowstr += "\n"
                tmp_file.write(rowstr.encode('utf-8'))

            logger.info("Done writing, importing at %s", datetime.datetime.now())
            tmp_file.seek(0)

            # attempt to copy the data into postgres
            # if it fails because the target table doesn't exist
            # try to create it by running self.create_table
            for attempt in range(2):
                try:
                    cursor = connection.cursor()
                    self.init_copy(connection)
                    self.copy(cursor, tmp_file)
                    self.post_copy(connection)
                except vertica_python.errors.MissingRelation:
                    if attempt == 0:
                        # if first attempt fails with "relation not found", try creating table
                        logger.info("Creating table %s", self.table)
                        connection.reset_connection()
                        self.create_table(connection)
                    else:
                        raise
                else:
                    break

            # mark as complete in same transaction
            self.output().touch(connection)


class VerticaQuery(rdbms.Query):
    """
    Template task for querying a Vertica compatible database

    Usage:
    Subclass and override the required `host`, `database`, `user`, `password`, `table`, and `query` attributes.

    Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once

    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """

    def run(self):
        with self.output().connect() as connection:
            cursor = connection.cursor()
            sql = self.query

            logger.info('Executing query from task: {name}'.format(name=self.__class__))
            cursor.execute(sql)

            # Update marker table
            self.output().touch(connection)

    def output(self):
        """
        Returns a VerticaTarget representing the executed query.

        Normally you don't override this.
        """
        return VerticaTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id
        )
