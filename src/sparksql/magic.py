from pyspark import SQLContext
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)
from findInst import find_insts
from prettyDataFrame import PrettyDataFrame
import re

@magics_class
class SparkSqlMagic(Magics):
    """Runs SQL statement through Spark using provided SQLContext.

    Provides the %sparksql magic.
    """

    context = None

    def __init__(self, shell):
        Magics.__init__(self, shell=shell)

    #TODO fix help doc, currently has execute shown for usage
    #TODO auto-detect file format?
    #TODO export
    @magic_arguments()
    @argument('-s', '--sqlcontext', help='SQLContext to use')
    @argument('-j', '--json', help='JSON file to read as table')
    @argument('-p', '--parquet', help='Parquet file to read as table')
    @argument('sql', type=str, default=["SHOW", "TABLES"], nargs='*', help='SQL to execute!')
    @line_magic('sparksql')
    @cell_magic('sparksql')
    def execute(self, line, cell = ''):
        """Runs a SQL statement through Spark using provided SQLContext.

        This magic will use the SQLContext specified using the -s argument.
        If none is provided, the magic will search the user namespace fo a
        SQLContext. If the magic finds exactly one SQLContext, it will be used.
        If there are multiple SQLContexts, one will need to be specified.
        This magic returns a pretty printing pyspark DataFrame.
        The -j and -p flags are used to load json and parquet files.
        The file will be loaded and registered as a table, with the table
        name inferred from the filename. Files must have .json or .parquet
        extension.

        Examples::
            
            %sparksql -s context SHOW TABLES

            %sparksql SELECT column FROM mytable

            %sparksql -j /foo/bar/qaz.json SELECT * FROM qaz

            %sparksql -p example.parquet

            %%sparksql -s context
            DROP TABLE mytable;
            SHOW TABLES
        """

        command = line + " " + cell
        args = parse_argstring(self.execute, command)
        new_context_name = args.sqlcontext
        
        statements = self.parse_sql(args.sql)
        
        json = args.json
        parquet = args.parquet
        
        self.find_context(new_context_name)

        if json is not None:
            self.read_file(json, "json")

        if parquet is not None:
            self.read_file(parquet, "parquet")

        for statement in statements:
            res = self.context.sql(statement)

        return PrettyDataFrame(res)


    #TODO better string manipulation
    def parse_sql(self, command):
        sql = ' '.join(command)
        sql.replace("\n", " ")
        statements = sql.split(";")
        statements = [stmt for stmt in statements if len(stmt) > 0]
        return statements

    def find_context(self, new_context_name):
        if new_context_name is not None:
            new_context = self.shell.user_ns.get(new_context_name)
            if new_context is None:
                raise NameError("Could not find SQLContext '" + new_context_name + "'")
            else:
                self.context = new_context
        
        elif self.context is None: 
            # Search for context
            contexts = find_insts(self.shell.user_ns, SQLContext)
            if len(contexts) == 0:
                raise ValueError("No SQLContext specified with -s and could not find one in local namespace")
            elif len(contexts) == 1:
                self.context = contexts.values()[0]
                print self.context
            elif len(contexts) > 1:
                raise ValueError("SQLContext must be specified with -s when there are multiple SQLcontexts in the local namespace")
        

    def read_file(self, filename, form):
        df = self.context.read.load(filename, format=form)

        table_name = self.get_table_name(filename, form)

        #TODO check for table name collision?
        df.registerAsTable(table_name)

        print("Stored " + filename + " in table " + table_name)

    def get_table_name(self, filename, extension):
        #TODO much better file name checking
        p = re.compile(r"([^\/\\\.]*)\." + extension + "$")
        
        m = p.search(filename)

        #TODO error handling
        return m.group(1)

def load_ipython_extension(ip):
    ip.register_magics(SparkSqlMagic)
