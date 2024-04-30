print("EXPORT SQL SERVER to QUOTED CSV.py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04" #example: SQL04
database = "TEMP_OK" #example: TEMP_OK

#There are 2 ways to search for tables to export...
# IF OPTION 1 IS FILLED OUT, OPTION 1 WILL BE USED
# IF OPTION 1 IS KEPT = [], OPTION 2 WILL BE USED

#--------------------------------------------------------
# OPTION 1 - SEARCH BY LIST:
#format as a list of strings ["table1", "table2", "etc..."]
list_of_table_names = [""]
#	"TABLENAME_INTERNAL_RAW"  #-OG RAW FILE
#	"TABLENAME-RAW-FILE"      #-RAW-FILE
#	"TABLENAME-FACEBOOK FILE" #-FINAL FILE
#]
#--------------------------------------------------------

#--------------------------------------------------------
# OPTION 2 - SEARCH BY REGEX:
#REGEX TEMPLATE TO GET ALL OF AN ORDER'S TABLES IF THEY'RE USING dash - or underscore _
#just copy it between the quotes after table_name_re = and replace order numbers as needed
#GPPO243510[-_](1$|1[-_])
table_name_re = ""
#--------------------------------------------------------





#USUALLY LEAVE THESE AS IS
order_by = ['PRIORITY']
order_by_asc = [True]
quoted = True

qualifier = '"'
extension = "csv"
seperator = ","
seperator_description = "comma"

# USER INPUT SECTION END
###################################################################





###################################################################
# SET UP
###################################################################
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect
import collections
from collections import defaultdict
import pandas as pd
import os
import csv
import re

engine = create_engine("mssql+pyodbc://:@" + str(server) + "/" + str(database) + "?driver=SQL+Server", use_setinputsizes=False)
inspector = inspect(engine)

###################################################################
# FIND TABLES IN SQL SERVER
###################################################################
if (list_of_table_names == [""] or list_of_table_names == []) and table_name_re != "":
	
  print("Searching for table matches in "+ str(server) + ".." + str(database) + " using regex expression " + str(table_name_re) + "...")
  
  list_of_table_names = []
  
  #for every dbo table
  for table_name in inspector.get_table_names('dbo'):
  	
  	#if the tablename matches the regex...
    if(re.search(table_name_re, str(table_name))):
    	
    	#add it to the listto export
      list_of_table_names.append(table_name)

  print(str(len(list_of_table_names)) + " tables found: " + str(list_of_table_names))
  print("")

###################################################################
# EXPORT
###################################################################
path = os.getcwd() #CURRENT DIRECTORY

i = 0
tables_exported_from_sql = 0

try:
 #for every table we found in the last step / for every table the user entered...
 for table_name in list_of_table_names:

  print("table " + str(i + 1) + ": " + str(table_name))

  with engine.connect() as conn:

    with open(str(path) + "\\" + str(table_name) + "." + str(extension), 'w', newline='') as csvfile:
    	
      print(server + ".." + database + ".[" + table_name + "] exporting...")

      #create file object and set quoting parameter
      if quoted == True:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL, delimiter=seperator)
      else:
        writer = csv.writer(csvfile, delimiter=seperator)

      #get the rows from the table
      rows = conn.execute(text("SELECT * FROM [" + str(table_name) + "]"))
      
      #parse columns for header
      columns_raw = str(rows.keys())
      columns_raw = columns_raw[12:]
      columns_raw = columns_raw[:-3]
      columns = columns_raw.split("', '")

      #build header row
      if quoted == True:
        header_row = '"'
        for column in columns:
          header_row = header_row + str(column) + '"' + str(seperator) + '"'
        header_row = header_row[:-2] #remove last quote and seperator
      else:
        header_row = ''
        for column in columns:
          header_row = header_row + str(column) + str(seperator)
        header_row = header_row[:-1] #remove last seperator

      #write header row
      csvfile.write(header_row + '\n')

      #write the rest of the rows
      for row in rows:
        writer.writerow(row)

      print(server + ".." + database + ".[" + table_name + "] exported as " + str(path) + "\\" + str(table_name) + "." + str(extension))
      

    try:
     if len(order_by) > 0:
      print("reading export csv to sort...")
      type = defaultdict(lambda: str)
      df = pd.read_csv(str(path) + "\\" + str(table_name) + "." + str(extension), dtype=type, keep_default_na=False, sep=seperator, quotechar=qualifier, encoding='latin-1')
      
      print("sorting...")
      df = df.sort_values(by=order_by, ascending=order_by_asc)
      print("writing sorted values...")
      if quoted == True:
        df.to_csv(str(path) + "\\" + str(table_name) + "." + str(extension), sep=seperator, quotechar=qualifier, encoding='utf-8', index=False, header=True, mode='w', quoting = csv.QUOTE_ALL)
      else:
        df.to_csv(str(path) + "\\" + str(table_name) + "." + str(extension), sep=seperator, quotechar=qualifier, encoding='utf-8', index=False, header=True, mode='w')
      print("done sorting.")
     print("")
    except Exception as e:
     print("ERROR")
     input(e)
    i = i + 1
    tables_exported_from_sql = tables_exported_from_sql + 1

    conn.commit()
except Exception as e:
 print(row)
 input(e)
###################################################################
# FINAL MESSAGE
###################################################################
try:
  print(str(tables_exported_from_sql) + " " + str(seperator_description) + "-delimited " + str(extension) + " files exported to " + str(path))
  print("")
except Exception as e:
  input(e)
input("Exit the Window")
