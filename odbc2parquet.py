#-------------------------------------------------------------------------------
#
#    odbc2parquet       Dump a table or query to a Parquet file
#
#                       Defaults to SQL Server ODBC driver, use DSN to support
#                       alternative DBMS.
#
#                       Fails by throwing exception so that we get full internal
#                       error details. Will change to try/except when mature.
#
#                       WARNING: Does not yet support all SQL Server data types,
#                                updates in progress.
#
#                       Setup:
#                       ------
#                       Install 64-bit Python3 (64-bit mandatory)
#                       Install SQL Server ODBC driver
#                       pip install pyodbc
#                       pip install pandas
#                       pip install pyarrow
#
#                       Execution:
#                       ----------
#                       python odbc2parquet <options>
#
#                       -D    --DSN           ODBC DSN
#                       -s    --server        Server (if not defined by DSN)
#                       -d    --database      Database (if not defined by DSN)
#                       -u    --user          User Name (if not integrated auth.)
#                       -p    --password      Password (if not integrated auth.)
#
#                       -o    --output        Output file name
#                                             Defaults to lightly edited table
#                                             name if not defined.
#
#                       -r    --rowgroup      Rowgroup Size
#                                             Defaults to 1,000,000
#
#                       -t    --table         [Schema.]Table to export
#
#                       -q    --query         SQL query to export
#                                             Must have aliased column names
#
#                             --debug         Print debug information
#
#                       Output:
#                       -------
#                       Output file name and number of rows exported to parquet
#
#                       Examples:
#                       ---------
#
#                       Simplest command line with integrated authentication to source:
#                       python odbc2parquet -D AzureAW -T SalesLT.SalesOrderHeader
#
#                       DSN requires SQL authentication:
#                       python odbc2parquet -D AzureAQ \
#                                           -u myUserName \
#                                           -p myPassword \
#                                           -t SalesLT.SalesOrderHeader
#
#                       Non-DSN connection:
#                       python odbc2parquet -s rondunn.database.windows.net \
#                                           -d AdventureWorks \
#                                           -u myUserName \
#                                           -p myPassword \
#                                           -t SalesLT.SalesOrderHeader
#
#                       Custom output file name, 1k rowgroup size:
#                       python odbc2parquet -s rondunn.database.windows.net \
#                                           -d AdventureWorks \
#                                           -u myUserName \
#                                           -p myPassword \
#                                           -t SalesLT.SalesOrderHeader \
#                                           -o test.parquet \
#                                           -r 1024
#
#                       Custom query, results to default 'query.parquet':
#                       python odbc2parquet -D AzureAW \
#                                           -q "select ProductID,Name from SalesLT.Product"
#
#-------------------------------------------------------------------------------

import argparse
import pandas
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
import decimal
import datetime
import time

# PyODBC data type handler

def handle_unknown_data_type (value):
	return str(value)

# Banner

print ()
print ('odbc2parquet: Export table/query to Parquet.')

timeStart = time.time()

# Set up command line

parser = argparse.ArgumentParser()
parser.add_argument ('-D','--DSN')
parser.add_argument ('-s','--server')
parser.add_argument ('-d','--database')
parser.add_argument ('-u','--user')
parser.add_argument ('-p','--password')
parser.add_argument ('-o','--output')
parser.add_argument ('-r','--rowgroup',type=int,default=1000000)
parser.add_argument ('-t','--table')
parser.add_argument ('-q','--query')
parser.add_argument ('--debug',action='store_true')
args = parser.parse_args()

# Validate arguments.

dsn = args.DSN
server = args.server
database = args.database
user = args.user
password = args.password
rowgroupSize = args.rowgroup
tableName = args.table

outputFileName = args.output
if outputFileName is None and tableName is not None:
    outputFileName = tableName.replace('.','_').lower() + '.parquet'
elif outputFileName is None and tableName is None:
    outputFileName = 'query.parquet'

if args.query is None and args.table is None:
	
    print ('Please specify TABLE (-t) or QUERY (-q)')
    exit (1)
    
query = args.query
if args.table is not None:
    query = f"select * from {args.table}"

# Connect to database

con = None
if dsn:
    constr = 'DSN=' + dsn
    if (user is not None and password is not None):
        constr = constr + f";UID={user};PWD={password}"
    con = pyodbc.connect(constr,readonly=True)
else:
    con = pyodbc.connect(f"""DRIVER={{ODBC Driver 17 for SQL Server}};
                             SERVER={args.server};
                             DATABASE={args.database};
                             UID={args.user};
                             PWD={args.password}""",readonly=True) 

# Add handlers for unsupported data types

con.add_output_converter(-151, handle_unknown_data_type)
            
# Execute query
           
cur = con.cursor()
cur.execute (query)

# Derive PyArrow schema from query result set

fields = []
for c in cur.description:
    if args.debug is True:
        print (c)
    ct = c[1]
    pr = c[4]
    sc = c[5]
    if ct is int:
    	if pr == 3:
        	fields.append (pa.field (c[0],pa.int8(),nullable=c[6]))
    	elif pr == 5:
        	fields.append (pa.field (c[0],pa.int16(),nullable=c[6]))
    	elif pr == 10:
        	fields.append (pa.field (c[0],pa.int32(),nullable=c[6]))
    	else:
        	fields.append (pa.field (c[0],pa.int64(),nullable=c[6]))
    elif ct is decimal.Decimal:
        fields.append (pa.field (c[0],pa.decimal128(c[4],c[5]),nullable=c[6]))
    elif ct is float:
    	if pr == 53:
    		fields.append (pa.field (c[0],pa.float32(),nullable=[c[6]]))
    	else:
    		fields.append (pa.field (c[0],pa.float64(),nullable=[c[6]]))
    elif ct is str:
        fields.append (pa.field (c[0],pa.string(),nullable=c[6]))
    elif ct is bytearray:
        fields.append (pa.field (c[0],pa.binary(),nullable=c[6]))
    elif ct is datetime.datetime:
        fields.append (pa.field (c[0],pa.timestamp('ms'),nullable=c[6]))
    elif ct is bool:
        fields.append (pa.field (c[0],pa.bool_(),nullable=c[6]))
    
schema = pa.schema (fields)

# Process result set

rowcount = 0
writer = None
cols = None
while (True):
    
    # Fetch enough rows to fill a rowgroup
    
    res = cur.fetchmany(rowgroupSize)
    if not res:
        break
        
    if rowcount > 0:
        timeNow = time.time()
        elapsed = timeNow - timeStart
        rps = int(rowcount / elapsed)
        print (f"{rowcount} rows at {rps} rows per second.")
        
    # Convert rows to dataframe
    
    rows = []
    if cols is None:
        cols = [column[0] for column in cur.description]
    for r in res:
        rows.append(dict(zip(cols,r)))
    rowcount = rowcount + len(rows)
    df = pandas.DataFrame(rows)
    
    # Convert dataframe to Table
    
    table = pa.Table.from_pandas(df,schema)
    
    # Write table to Parquet
    
    if writer is None:
        writer = pq.ParquetWriter(outputFileName,schema) # ,coerce_timestamps='ms'
    writer.write_table (table)

if writer:
    writer.close()
    
# print final success message.

timeNow = time.time()
elapsed = timeNow - timeStart
rps = int(rowcount / elapsed)
print (f"{rowcount} rows exported to {outputFileName} at {rps} rows per second.")

if args.debug is True:
    print ()
    print ('Test output display:')
    print ()
    print ('Schema:')
    print (pq.read_schema(outputFileName))
    print ()
    testTable = pq.read_table (outputFileName)
    print (testTable.to_pandas())

exit(0)
