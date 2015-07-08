from nose.tools import raises, with_setup, assert_equals
from sparksql.fillPlaceholder import replace
ns = None 

def _setup():
    global ns
    ns = {"ident1" : "val1", "ident2" : "val2", "ident3" : 55}

@with_setup(_setup)
def test_single_placeholder_replaced():
    q = "SELECT :ident1 FROM place"
    replaced = replace(q, ns)
    assert_equals("SELECT val1 FROM place", replaced)

@with_setup(_setup)
def test_multiple_placeholders_replaced():
    q = "SELECT :ident1 FROM :ident2 WHERE :ident1=:ident3"
    replaced = replace(q, ns)
    assert_equals("SELECT val1 FROM val2 WHERE val1=55", replaced)

@raises(KeyError)
def test_not_found_placeholder_causes_exception():
    q = "SELECT :dummy FROM place"
    replace(q, ns)

@with_setup(_setup)
def test_replaces_in_single_quote_string_literal():
    q = "SELECT thing FROM place WHERE val=':ident1'"
    replaced = replace(q, ns)
    assert_equals("SELECT thing FROM place WHERE val='val1'", replaced)

@with_setup(_setup)
def test_replaces_in_double_quote_string_literal():
    q = 'SELECT thing FROM place WHERE val=":ident1"'
    replaced = replace(q, ns)
    assert_equals('SELECT thing FROM place WHERE val="val1"', replaced)

@with_setup(_setup)
def test_normal_string_literal_not_replaced():
    q = 'SELECT "column_name" FROM table'
    replaced = replace(q, ns)
    assert_equals(q, replaced)
