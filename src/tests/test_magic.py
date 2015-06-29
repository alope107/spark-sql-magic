from nose.tools import raises, with_setup, assert_equals
from sparksql.magic import SparkSqlMagic
from pyspark import SQLContext
from pyspark.sql import DataFrame
import os

ip = get_ipython()
sqlcon = None
sqlcon2 = None

class DummyCtx:
    _sc = "dummy"

class DummySQLContext(SQLContext):
    query = None
    read = None
    df = None
    def __init__(self):
        self.read = DummyLoader()
    
    def sql(self, query):
        self.query = query
        sql_ctx = DummyCtx()
        self.df = DummyDataFrame(None, sql_ctx)
        return self.df
    

class DummyDataFrame(DataFrame):
    table_name = None
    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx
    def registerAsTable(self, table_name):
        self.table_name = table_name

class DummyLoader():
    fname = None
    form = None
    df = None
    def load(self, fname, format=None):
        self.fname = fname
        self.form = format
        self.df = DummyDataFrame(None, DummyCtx())
        return self.df

def setup():
    global sqlcon, sqlcon2
    sqlcon = DummySQLContext()
    sqlcon2 = DummySQLContext()

def _setup():
    sparksqlmagic = SparkSqlMagic(shell=ip)
    ip.register_magics(sparksqlmagic)

def _teardown():
    ip.user_ns.pop("sqlcon", None)
    ip.user_ns.pop("sqlcon2", None)


@raises(ValueError)
@with_setup(_setup, _teardown)
def test_magic_throws_exception_if_no_SQLContext_present():
    ip.run_line_magic('sparksql', 'SHOW TABLES')

@with_setup(_setup, _teardown)
def test_magic_finds_SQLContext_and_passes_query():
    ip.user_ns["sqlcon"] = sqlcon
    query = 'SHOW TABLES'
    result = ip.run_line_magic('sparksql', query)
    assert(isinstance(result, DataFrame))
    assert_equals(query, sqlcon.query)
    

@raises(ValueError)
@with_setup(_setup, _teardown)
def test_magic_fails_on_multiple_SQLContext_present():
    ip.user_ns["sqlcon"] = sqlcon
    ip.user_ns["sqlcon2"] = sqlcon2
    ip.run_line_magic('sparksql', 'SHOW TABLES')

@with_setup(_setup, _teardown)
def test_magic_parses_context_argument_correctly():
    ip.user_ns["sqlcon"] = sqlcon
    ip.user_ns["sqlcon2"] = sqlcon2
    args = "-s sqlcon2 "
    query = "SELECT * FROM TABLE"
    ip.run_line_magic('sparksql', args + query)
    assert_equals(query, sqlcon2.query)

@with_setup(_setup, _teardown)
def test_load_json():
    ip.user_ns["sqlcon"] = sqlcon
    name = "people"
    form = "json"
    path = "/foo/bar/"
    full_name = path + name + "." + form
    args = "-j " + full_name + " "
    query = "SELECT * FROM people"

    print args+query
    ip.run_line_magic('sparksql', args + query)

    assert_equals(full_name, sqlcon.read.fname)
    assert_equals(form, sqlcon.read.form)
    assert_equals(name, sqlcon.read.df.table_name)

@with_setup(_setup, _teardown)
def test_load_parquet():
    ip.user_ns["sqlcon"] = sqlcon
    name = "people"
    form = "parquet"
    path = "/foo/bar/"
    full_name = path + name + "." + form
    args = "-p " + full_name + " "
    query = "SELECT * FROM people"

    print args+query
    ip.run_line_magic('sparksql', args + query)

    assert_equals(full_name, sqlcon.read.fname)
    assert_equals(form, sqlcon.read.form)
    assert_equals(name, sqlcon.read.df.table_name)
    
