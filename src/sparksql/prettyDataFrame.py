from pyspark.sql.dataframe import DataFrame
from StringIO import StringIO
import sys

class PrettyDataFrame(DataFrame):
    """A pyspark DataFrame that pretty-prints.
    
    Uses the show() method of the base DataFrame."""
    
    def __init__(self, base):
        super(PrettyDataFrame, self).__init__(base._jdf, base.sql_ctx)
    
    def __repr__(self):
        # Capture output of show 
        oldstdout = sys.stdout
        sys.stdout = newstdout = StringIO()

        self.show()

        sys.stdout = oldstdout
        return newstdout.getvalue()
