print("IMPORT FIXED TXT to SQL SERVER VARCHAR(255).py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SERVER_NAME" #example: SQL04
database = "DB_NAME" #example: TEMP_OK

#Keep "column_specs = []" below as is if you want pandas to automatically infer the column widths
#I've found this works if there are spaces between the columns

#If you'd like to manually enter column widths (0-indexed), please follow this format:
#column_specs = [
#  (0,1), #column 1 (start inclusive, end exclusive) ... so (0, 1) means only the first character in each row is column 1
#  (1,2), #column 2
#  (2,3)  #column 3 etc...
#]

column_specs = []
extension = "txt"
seperator_description = "fixed-width"

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

    #if it's a txt...
    if filename.endswith(".txt"): 
    	
        print(str(path) + "\\" + str(filename) + " importing...")

        #get path and name w/o extension
        full_path = str(path) + "\\" + str(filename)
        filename_no_ext = os.path.splitext(os.path.basename(filename))[0]

        with engine.connect() as conn:

          #set up table for import
          type = defaultdict(lambda: str)

          #read fixed txt file
          #if user wants panda to infer colspecs
          if column_specs == []: 
            df = pd.read_fwf(full_path, dtype=type, keep_default_na=False, colspecs="infer")
          #if user entered colspecs
          else: 
            df = pd.read_fwf(full_path, dtype=type, keep_default_na=False, colspecs=column_specs)
     
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
print(str(tables_imported_to_sql) + " " + str(seperator_description) + " " + str(extension) + " files imported to " + str(server) + ".." + str(database) + " as tables")
print("")

input("Exit the Window")