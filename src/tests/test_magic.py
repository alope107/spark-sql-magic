from nose.tools import raises, with_setup
from sparksql.magic import SparkSqlMagic
from pyspark import SQLContext

ip = get_ipython()
sqlcon = None
sqlcon2 = None

# Used to avoid launching Spark when unessecary
class DummySQLContext(SQLContext):
    def __init__(self):
        pass
    def sql(self, query):
        return "dummy"

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

# Currently fails erroneously
# TODO figure out how to test properly in IPython
@with_setup(_setup, _teardown)
def test_magic_finds_SQLContext():
    ip.user_ns["sqlcon"] = sqlcon
    should_be_in_ns = "it should, right?"
    ip.run_line_magic('sparksql', 'SHOW TABLES')

@raises(ValueError)
@with_setup(_setup, _teardown)
def test_magic_fails_on_multiple_SQLContext_find():
    ip.user_ns["sqlcon"] = sqlcon
    ip.user_ns["sqlcon2"] = sqlcon2
    ip.run_line_magic('sparksql', 'SHOW TABLES')
