import os

def load_file(filename, context):
    table, ext = parse_file_name(filename)

    df = context.read.load(filename, format=ext)

    #TODO check for table name collision?
    df.registerAsTable(table)

    print("Stored " + filename + " in table " + table)

def write_file(df, filename):
    ext = parse_file_name(filename)[1]
    df.write.save(filename, format=ext)

def parse_file_name(filename):
    basename = os.path.basename(filename)
    table, ext = os.path.splitext(basename)

    #remove leading period
    ext = ext[1:]
    return table, ext
