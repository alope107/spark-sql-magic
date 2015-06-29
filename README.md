# spark-sql-magic
Runs a SQL statement through Spark using provided SQLContext.

This magic will use the SQLContext specified using the -s argument.
If none is provided, the magic will search the user namespace fo a
SQLContext. If the magic finds exactly one SQLContext, it will be used.
If there are multiple SQLContexts, one will need to be specified.
This magic returns a pretty printing pyspark DataFrame.
The -j and -p flags ar used to load json and parquet files.
The file will be loaded and registered as a table, with the table
name inferred from the filename. Files must have .json or .parquet
extension.

Examples:
            
    %sparksql -s context SHOW TABLES

    %sparksql SELECT column FROM mytable

    %sparksql -j /foo/bar/qaz.json SELECT * FROM qaz

    %sparksql -p example.parquet
