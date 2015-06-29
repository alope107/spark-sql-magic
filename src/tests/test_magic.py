from nose.tools import raises, with_setup, assert_equals
from sparksql.magic import SparkSqlMagic
from pyspark import SQLContext
from pyspark.sql import DataFrame

ip = get_ipython()
sqlcon = None
sqlcon2 = None

class DummyCtx:
    _sc = "dummy"

class DummySQLContext(SQLContext):
    query = None
    def __init__(self):
        pass
    def sql(self, query):
        self.query = query
        sql_ctx = DummyCtx()
        return DataFrame("dummy", sql_ctx)

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
    query = "DROP * FROM TABLE"
    ip.run_line_magic('sparksql', args + query)
    assert_equals(query, sqlcon2.query)
