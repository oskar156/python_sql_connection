print("IMPORT EXCEL to SQL SERVER VARCHAR(255).py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04" #example: SQL04
database = "TEMP_OK" #example: TEMP_OK





#USUALLY LEAVE THESE AS IS
extensions = ['xls', 'xlsx', 'xlsm'] #other .xl* extensions not tested

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
import openpyxl
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

    #if it's in the extension list (if it's an excel file):
    valid_file = False
    try:
      filename_split = filename.split('.')
      current_file_extension = filename_split[len(filename_split) - 1]
      
      #if this throws an exception then valid_file will stay false and this file will be skipped
      extensions.index(str(current_file_extension))
      valid_file = True
    except:
      valid_file = False
    
    
    if valid_file == True: 
    	
        #print(str(path) + "\\" + str(filename) + " importing...")

        #get path and name w/o extension
        full_path = str(path) + "\\" + str(filename)
        filename_no_ext = os.path.splitext(os.path.basename(filename))[0]

        excel_file = pd.ExcelFile(full_path)
        
        number_of_sheets = len(excel_file.sheet_names)
        
        for sheet in excel_file.sheet_names:
          print(str(path) + "\\" + str(filename) + " - " + str(sheet) + " importing...")

          type = defaultdict(lambda: str)
          df = pd.read_excel(full_path, sheet, dtype=type, keep_default_na = False)

          #remove all non-standard characters
          df = df.replace(r'[^A-z0-9\(\)\[\]\{\}`~\.!?@#$%^&\*-_=+\\/|:;\"\',<>\t\ ]', '', regex=True)
          	
          #if an excel sheet has a single sheet then it'll be named just after the spreadsheet
          #if it has more than 1 then it'll be named after the spreadsheet and sheet
          if number_of_sheets == 1:
            import_table_name = str(filename)
          elif number_of_sheets > 1:
            import_table_name = str(filename) + " - " + str(sheet)
          
          with engine.connect() as conn:

            #import table and set each column to NVARCHAR(length=255), no index column, and append if exists
            df.to_sql(import_table_name , schema='dbo', con=engine, dtype={col_name: NVARCHAR(length=255) for col_name in df}, index=False, if_exists='append')
          	
            #necessary to finalize the import
            conn.commit()

          print(str(path) + "\\" + str(filename) + " - " + str(sheet) + " imported as [" + str(import_table_name) + "]")
          tables_imported_to_sql = tables_imported_to_sql + 1

    else:
        print(str(path) + "\\" + str(filename) + " skipped because it's not an excel file.")

    print("")

    i = i + 1

###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_imported_to_sql) + " excel files imported to " + str(server) + ".." + str(database))
print("")

input("Exit the Window")
