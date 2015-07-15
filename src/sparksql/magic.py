from pyspark import SQLContext
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)
from prettyDataFrame import PrettyDataFrame
import re
import os.path
import sqlparse
from fillPlaceholder import replace
from fileManip import load_file, write_file
from IPython.core.display import display_javascript

@magics_class
class SparkSqlMagic(Magics):
    """Runs SQL statement through Spark using provided SQLContext.

    Provides the %sparksql magic.
    """

    def __init__(self, shell):
        Magics.__init__(self, shell=shell)
        self.context = None
        self.user_ns = None


    #TODO fix help doc, currently has execute shown for usage
    #TODO support for custom data sources
    @magic_arguments()
    @argument('-s', '--sqlcontext', help='SQLContext to use')
    @argument('-l', '--load', help='File to read as table')
    @argument('-w', '--write', help='File to write output to')
    @argument('sql', type=str, default=["SHOW", "TABLES"], nargs='*', help='SQL to execute!')
    @needs_local_scope
    @line_magic('sparksql')
    @cell_magic('sparksql')
    def execute(self, line, cell = '', local_ns={}):
        """Runs a SQL statement through Spark using provided SQLContext.

        This magic will use the SQLContext specified using the -s argument.
        If none is provided, the magic will search the user namespace fo a
        SQLContext. If the magic finds exactly one SQLContext, it will be used.
        If there are multiple SQLContexts, one will need to be specified.
        This magic returns a pretty printing pyspark DataFrame.
        The -l option is used to load json and parquet files.
        The file will be loaded and registered as a table, with the table
        name inferred from the filename. Files must have .json or .parquet
        extension. The -w option is used to write the output of a query
        to a JSON or parquet file. The output format is inferred from the 
        file extension. Python variables can be referenced by prepending their
        identifiers with a colon. This will inject the string representation
        of the variable into the query.

        Examples::
            
            %sparksql -s context SHOW TABLES

            %sparksql SELECT column FROM mytable

            %sparksql -l /path/to/input.json SELECT * FROM qaz

            %sparksql -l example.parquet

            %%sparksql -s context
            DROP TABLE mytable;
            SHOW TABLES;

            %sparksql -w /path/to/output.json SELECT column, otherColumn FROM table

            myvar = 10
            %sparksql SELECT * FROM table WHERE value < :myvar
        """

        self.user_ns = self.shell.user_ns.copy()
        self.user_ns.update(local_ns)

        command = line + " " + cell
        args = parse_argstring(self.execute, command)
        new_context_name = args.sqlcontext
        
        statements = self.parse_sql(args.sql, self.user_ns)
        
        to_load = args.load
        to_write = args.write
        
        self.find_context(new_context_name)

        if to_load:
            load_file(to_load, self.context)

        for statement in statements:
            res = self.context.sql(statement)

        if to_write:
            write_file(res, to_write)

        return PrettyDataFrame(res)


    def parse_sql(self, command, ns):
        sql = ' '.join(command)
        statements = [replace(str(statement).rstrip(';'), ns) for statement in sqlparse.split(sql)]
        return statements

    def find_context(self, new_context_name):
        if new_context_name is not None:
            new_context = self.user_ns.get(new_context_name)
            if new_context is None:
                raise NameError("Could not find SQLContext '" + new_context_name + "'")
            else:
                self.context = new_context
        
        elif self.context is None: 
            # Search for context
            contexts = self.find_insts(self.user_ns, SQLContext)
            if len(contexts) == 0:
                raise ValueError("No SQLContext specified with -s and could not find one in local namespace")
            elif len(contexts) == 1:
                self.context = contexts.values()[0]
            elif len(contexts) > 1:
                raise ValueError("SQLContext must be specified with -s when there are multiple SQLcontexts in the local namespace")

    def find_insts(self, namespace, to_find):
        return {k:v for k, v in namespace.iteritems() if isinstance(v, to_find)}

def load_ipython_extension(ip):
    js = "IPython.CodeCell.config_defaults.highlight_modes['magic_text/x-sql'] = {'reg':[/^%%sparksql/]};"
    display_javascript(js, raw=True)
    ip.register_magics(SparkSqlMagic)
