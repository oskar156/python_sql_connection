print("IMPORT COMMA TXT to SQL SERVER VARCHAR(255).py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04" #example: SQL04
database = "TEMP_OK" #example: TEMP_OK





#USUALLY LEAVE THESE AS IS
qualifier = '"'
seperator = ","
seperator_description = "comma"
extension = "txt"

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
import re

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
  
  if str(filename[-3:]) != '.py':
    
    print("file " + str(i + 1) + ": " + str(filename))

    #if it's a txt...
    if filename.endswith("." + str(extension)): 
    	
        print(str(path) + "\\" + str(filename) + " importing...")

        #get path and name w/o extension
        full_path = str(path) + "\\" + str(filename)
        filename_no_ext = os.path.splitext(os.path.basename(filename))[0]

        with engine.connect() as conn:

          #set up table for import, separate by , comma
          #encoding='latin-1' to avoid the script crashing when running into non-standard characters
          type = defaultdict(lambda: str)
          df = pd.read_csv(full_path, dtype=type, keep_default_na=False, sep=seperator, quotechar=qualifier, encoding='latin-1')

          #remove all non-standard characters
          df = df.replace(r'[^A-z0-9\(\)\[\]\{\}`~\.!?@#$%^&\*-_=+\\/|:;\"\',<>\t\ ]', '', regex=True)
          	
          #import table and set each column to NVARCHAR(length=255), no index column, and append if exists
          df.to_sql(filename_no_ext , schema='dbo', con=engine, dtype={col_name: NVARCHAR(length=255) for col_name in df}, index=False, if_exists='append')
          	
          #necessary to finalize the import
          conn.commit()

        print(str(path) + "\\" + str(filename) + " imported as [" + str(filename_no_ext) + "]")

        tables_imported_to_sql = tables_imported_to_sql + 1

    else:
        print(str(path) + "\\" + str(filename) + " skipped because it's not a txt file.")

    print("")

    i = i + 1

###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_imported_to_sql) + " " + str(seperator_description) + "-delimited " + str(extension) + " files imported to " + str(server) + ".." + str(database))
print("")

input("Exit the Window")
