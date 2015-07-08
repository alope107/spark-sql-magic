from sqlparse import parse
from sqlparse.sql import Identifier
from sqlparse.tokens import Name, Token

def replace(sqlstring, ns):
    """
    Replaces placeholders in a SQL string using values in namespace.

    Placeholders are identified by a colon prepended to the
    key in the namespace. For example, to get the bound value of
    the variable foo

    `SELECT :foo FROM table

    If there is a value of foo that can be given a string representation
    it will be placed into the string.  Values can also be inserted
    as a string literal in the SQL statement if they are enclosed by quotes

    `SELECT :foo FROM table WHERE value=":bar"

    This function will throw a key error if there is no value for a
    key referenced in the statement.
    """
    return replace_recr(parse(sqlstring)[0], ns).to_unicode()

def replace_recr(stmt, ns):
    try:
        #if it has tokens, it is not a leaf node
        tokens = stmt.tokens
        
        for n, token in enumerate(tokens):
            stmt.tokens[n] = replace_recr(token, ns)
        return stmt
    
    #If it does not have tokens, it is a leaf node
    except AttributeError:
        
        if stmt.ttype is Name.Placeholder:
            name = stmt.to_unicode()
            if name[0] == ":":
                name = var_value(name[1:], ns)
            return Identifier(name)

        if stmt.ttype is Token.Literal.String.Single or Token.Literal.String.Symbol:
            lit = stmt.to_unicode()
            
            #Ensure that this is a string literal
            if lit[0] == "'" or lit[0] == '"':
                if lit[1] == ":":
                    #Strip quotes and colon before lookup
                    val = var_value(lit[2:-1], ns)
                    #Re-apply quotes
                    lit = lit[0] + val + lit[-1]
            return Identifier(lit)
        #If it is not a placeholder or literal, return unchanged
        return stmt


def var_value(varname, ns):
    try:
        return ns[varname].__str__()
    except KeyError:
        #Re-raises to give more helpful message
        raise KeyError("Could not find variable " + varname + " in namespace.")
