# spark-sql-magic
Runs a SQL statement through Spark using provided SQLContext.

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

    %sparksql -l /foo/bar/qaz.json SELECT * FROM qaz

    %sparksql -l example.parquet

    %%sparksql -s context
    DROP TABLE mytable;
    SHOW TABLES;

    %sparksql -w /path/to/output.json SELECT column, otherColumn FROM table

    myvar = 10
    %sparksql SELECT * FROM table WHERE value < :myvar
