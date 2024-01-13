print("IMPORT CSV to SQL SERVER VARCHAR(255).py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SERVER_NAME" #example: SQL04
database = "DB_NAME" #example: TEMP_OK

qualifier = '"'
seperator = ","
seperator_description = "comma"
extension = "csv"

# USER INPUT SECTION END
###################################################################





###################################################################
# SET UP
###################################################################
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.types import NVARCHAR
import collections
from collections import defaultdict
import pandas as pd
import os

engine = create_engine("mssql+pyodbc://:@" + str(server) + "/" + str(database) + "?driver=SQL+Server", use_setinputsizes=False, fast_executemany=True)

###################################################################
# IMPORT
###################################################################
path = os.getcwd() #CURRENT DIRECTORY

i = 0
tables_imported_to_sql = 0

#for every file in the current directory...
for file in os.listdir(path):

    #open file to import
    filename = os.fsdecode(file)
    
    print("file " + str(i + 1) + ": " + str(filename))

    #if it's a csv...
    if filename.endswith(".csv"): 
    	
        print(str(path) + "\\" + str(filename) + " importing...")

        #get path and name w/o extension
        full_path = str(path) + "\\" + str(filename)
        filename_no_ext = os.path.splitext(os.path.basename(filename))[0]

        with engine.connect() as conn:

          #set up table for import, separate by comma ,
          type = defaultdict(lambda: str)
          df = pd.read_csv(full_path, dtype=type, keep_default_na=False, sep=seperator, quotechar=qualifier)
          
          #import table and set each column to NVARCHAR(length=255), no index column, and append if exists
          df.to_sql(filename_no_ext , schema='dbo', con=engine, dtype={col_name: NVARCHAR(length=255) for col_name in df}, index=False, if_exists='append')
          	
          #necessary to finalize the import
          conn.commit()

        print(str(path) + "\\" + str(filename) + " imported as [" + str(filename_no_ext) + "]")

        tables_imported_to_sql = tables_imported_to_sql + 1

    else:
        print(str(path) + "\\" + str(filename) + " skipped because it's not a csv file.")

    print("")
    i = i + 1

###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_imported_to_sql) + " " + str(seperator_description) + "-delimited " + str(extension) + " files imported to " + str(server) + ".." + str(database) + " as tables")
print("")

input("Exit the Window")