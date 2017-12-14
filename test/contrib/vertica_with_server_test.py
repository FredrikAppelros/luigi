# -*- coding: utf-8 -*-

from helpers import unittest
from nose.plugins.attrib import attr

import luigi

from luigi.contrib import vertica


host = 'localhost'
database = 'vertica'
user = 'dbadmin'
password = ''


try:
    import pyodbc
    conn = pyodbc.connect(
        driver='vertica',
        user=user,
        host=host,
        database=database,
        password=password
    )
    conn.close()
except Exception:
    raise unittest.SkipTest('Unable to connect to Vertica')


class CopyToTestDB(vertica.CopyToTable):
    host = host
    database = database
    user = user
    password = password


class TestVerticaTask(CopyToTestDB):
    table = 'test_table'
    columns = [
        ('test_text', 'varchar'),
        ('test_int', 'int'),
        ('test_float', 'float')
    ]

    def create_table(self, connection):
        connection.cursor().execute(
            "CREATE TABLE {} "
            "(id IDENTITY PRIMARY KEY, test_text VARCHAR, test_int INT, test_float FLOAT)"
            .format(self.table)
        )

    def rows(self):
        yield 'foo', 123, 123.45
        yield None, '-100', '5143.213'
        yield u'éцү我', 0, 0
        yield None, 0, ''


class MetricBase(CopyToTestDB):
    table = 'metrics'
    columns = [
        ('metric', 'varchar'),
        ('value', 'int')
    ]


class Metric1(MetricBase):
    param = luigi.Parameter()

    def rows(self):
        yield 'metric1', 1
        yield 'metric1', 2
        yield 'metric1', 3


class Metric2(MetricBase):
    param = luigi.Parameter()

    def rows(self):
        yield 'metric2', 1
        yield 'metric2', 4
        yield 'metric2', 3


def rows_to_tuples(rows):
    return [tuple(r) for r in rows]


@attr('vertica')
class TestVerticaImportTask(unittest.TestCase):

    def test_repeat(self):
        task = TestVerticaTask()
        conn = task.output().connect()
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS {}'.format(task.table))
        cursor.execute('DROP TABLE IF EXISTS {}'.format(vertica.VerticaTarget.marker_table))

        luigi.build([task], local_scheduler=True)
        luigi.build([task], local_scheduler=True)  # try to schedule twice

        cursor.execute(
            "SELECT test_text, test_int, test_float "
            "FROM test_table "
            "ORDER BY id ASC"
        )

        rows = cursor.fetchall()

        self.assertEqual(rows_to_tuples(rows), [
            (u'foo', 123, 123.45),
            (None, -100, 5143.213),
            (u'éцү我', 0, 0.0),
            (None, 0, None)
        ])

    def test_multimetric(self):
        metrics = MetricBase()
        conn = metrics.output().connect()
        conn.cursor().execute('DROP TABLE IF EXISTS {}'.format(metrics.table))
        conn.cursor().execute('DROP TABLE IF EXISTS {}'.format(vertica.VerticaTarget.marker_table))
        luigi.build([Metric1(20), Metric1(21), Metric2("foo")], local_scheduler=True)

        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM {}'.format(metrics.table))
        self.assertEqual(rows_to_tuples(cursor.fetchall()), [(9,)])

    def test_clear(self):
        class Metric2Copy(Metric2):

            def init_copy(self, connection):
                query = "TRUNCATE TABLE {}".format(self.table)
                connection.cursor().execute(query)

        clearer = Metric2Copy(21)
        conn = clearer.output().connect()
        conn.cursor().execute('DROP TABLE IF EXISTS {}'.format(clearer.table))
        conn.cursor().execute('DROP TABLE IF EXISTS {}'.format(vertica.VerticaTarget.marker_table))

        luigi.build([Metric1(0), Metric1(1)], local_scheduler=True)
        luigi.build([clearer], local_scheduler=True)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM {}'.format(clearer.table))
        self.assertEqual(rows_to_tuples(cursor.fetchall()), [(3,)])
