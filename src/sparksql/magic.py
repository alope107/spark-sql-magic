from pyspark import SQLContext
from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)
from findInst import find_insts
from prettyDataFrame import PrettyDataFrame

@magics_class
class SparkSqlMagic(Magics):
    """Does magic"""

    context = None

    def __init__(self, shell):
        Magics.__init__(self, shell=shell)

    ##TODO fix help doc
    @magic_arguments()
    @argument('-s', '--sqlcontext', help='SQLContext to use')
    @argument('sql', type=str, nargs='+', help='SQL to execute!')
    @needs_local_scope
    @line_magic('sparksql')
    @cell_magic('sparksql')
    def execute(self, line, cell = '', local_ns={}):
        """Executes some sql through spark"""


        print "***********GOT LOCAL_NS*************" + str(local_ns)
        args = parse_argstring(self.execute, line)
        new_context_name = args.sqlcontext
        new_context = local_ns.get(new_context_name)
        sql = ' '.join(args.sql)
        
        if new_context_name is not None:
            if new_context is None:
                raise NameError("Could not find SQLContext '" + new_context_name + "'")
            else:
                self.context = new_context
        elif self.context is None: 
            # Search for context
            contexts = find_insts(local_ns, SQLContext)
            if len(contexts) == 0:
                raise ValueError("No SQLContext specified with -s and could not find one in local namespace")
            elif len(contexts) == 1:
                self.context = contexts.values()[0]
            elif len(contexts) > 1:
                raise ValueError("SQLContext must be specified with -s when there are multiple SQLcontexts in the local namespace")

        res = self.context.sql(sql)

        return PrettyDataFrame(res)

def load_ipython_extension(ip):
    ip.register_magics(SparkSqlMagic)
