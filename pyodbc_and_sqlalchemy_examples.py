######################
# NOTES
######################
#cnxn and conn_str from:
#  https://stackoverflow.com/questions/46045834/pyodbc-data-source-name-not-found-and-no-default-driver-specified

#CURSOR OBJECTS
#  https://www.tutorialspoint.com/python_data_access/python_mysql_cursor_object.htm
#  https://code.google.com/archive/p/pyodbc/wikis/Cursor.wiki

#IDEAS:
#  EXPORT A TABLE TO CSV: https://stackoverflow.com/questions/42844895/pyodbc-query-results-to-csv

######################
# SET UP THE CONNECTION
######################
import pyodbc

conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=SQL04;'
    r'DATABASE=TEMP_OK;'
    r'Trusted_Connection=yes;'
)

cnxn = pyodbc.connect(conn_str)

######################
# EXAMPLE 1 - PRINT ROWS IN TABLE
######################
cursor = cnxn.cursor()
cursor.execute("SELECT * FROM [COUNT1219231141 - HERITAGE HILL SCOTTSBLUFF]")
for row in cursor.fetchall():
    print(row)

######################
# EXAMPLE 2 - GET ROW COUNT
######################
cnxn.execute("SELECT COUNT(*) FROM [COUNT1219231141 - HERITAGE HILL SCOTTSBLUFF]").fetchall() 
#[(38,)]

######################
# EXAMPLE 3 - GET ROWS AS A LIST
######################
cnxn.execute("SELECT * FROM [COUNT1219231141 - HERITAGE HILL SCOTTSBLUFF]").fetchall()
#[('CURRENT RESIDENT', '50037 LEISURE LN', 'SCOTTSBLUFF', 'NE', 69361), ('CURRENT RESIDENT', '50302 LE etc...

######################
# EXAMPLE 4 - READ FROM A FILE AND INSERT INTO EXISTING TABLE
######################
file = open(r'C:\Users\oscar\Desktop\names.csv')
for line in file:
  line_split = line.split(",")
  col0 = strip(line_split[0])
  col1 = strip(line_split[1])
  cursor = cnxn.cursor()
  cursor.execute("INSERT INTO pyodbc_test select '" + str(col0) + "','" + str(col1) + "' ")
  cnxn.commit()

######################
# EXAMPLE 5 - IMPORT NEW TABLE TO SQL
######################
#https://stackoverflow.com/questions/25661754/get-data-from-pandas-into-a-sql-server-with-pyodbc
#https://docs.sqlalchemy.org/en/20/tutorial/engine.html

import sqlalchemy
from sqlalchemy import text
from sqlalchemy import create_engine

engine = create_engine("mssql+pyodbc://:@SQL04/TEMP_OK?driver=SQL+Server", use_setinputsizes=False)

with engine.connect() as conn:
  df = pd.read_csv(r'C:\Users\oscar\Desktop\names.csv')
  df.to_sql('PANDAS_IMPORT_TEST', schema='dbo', con=engine)
  conn.execute(text("DROP INDEX ix_dbo_PANDAS_IMPORT_TEST_index ON PANDAS_IMPORT_TEST"))
  conn.execute(text("alter table PANDAS_IMPORT_TEST drop column [index]"))
  conn.commit()

######################
# EXAMPLE 6 - EXPORT TABLE TO CSV FROM SQL
######################
import csv
import pyodbc

conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=SQL04;'
    r'DATABASE=TEMP_OK;'
    r'Trusted_Connection=yes;'
)
cnxn = pyodbc.connect(conn_str)
crsr = cnxn.cursor()

sql = "SELECT * FROM PANDAS_IMPORT_TEST"
rows = crsr.execute(sql)

with open(r'C:\Users\oscar\Desktop\exporttest.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([x[0] for x in crsr.description])  # column headers
    for row in rows:
        writer.writerow(row)









##OTHER STUFF
with engine.connect() as conn:
  conn.execute(text("INSERT INTO pyodbc_test SELECT 'Jerry','Allen'"))
  conn.commit()

engine.connect()
with engine.connect() as conn:
  result = conn.execute(text("select 'hello world'"))
  print(result.all())
