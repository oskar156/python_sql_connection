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
list_of_table_names = ["MLTEST_VOTER_1_TRAIN"]
#--------------------------------------------------------

#--------------------------------------------------------
# OPTION 2 - SEARCH BY REGEX:
#REGEX TEMPLATE TO GET ALL OF AN ORDER'S TABLES IF THEY'RE USING dash - or underscore _
#just copy it between the quotes after table_name_re = and replace order numbers as needed
#GPPO243510[-_](1$|1[-_])
table_name_re = ""
#--------------------------------------------------------





#USUALLY LEAVE THESE AS IS
quoted = True
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
from sqlalchemy import inspect
import os
import csv
import re
import pandas as pd

engine = create_engine("mssql+pyodbc://:@" + str(server) + "/" + str(database) + "?driver=SQL+Server", use_setinputsizes=False)
inspector = inspect(engine)

###################################################################
# FIND TABLES IN SQL SERVER
###################################################################
if list_of_table_names == [] and table_name_re != "":
	
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
	
  unique_values = []
  unique_codes = []
  columns_to_code = []
  
  print("table " + str(i + 1) + ": " + str(table_name))

  with open(str(path) + "\\" + str(table_name) + "." + str(extension), 'w', newline='') as csvfile:
    	
      print(server + ".." + database + ".[" + table_name + "] exporting...")

      #create file object and set quoting parameter
      if quoted == True:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL, delimiter=seperator)
      else:
        writer = csv.writer(csvfile, delimiter=seperator)

      #get the rows from the table

      with engine.connect() as conn:
        print(table_name)
        rows = conn.execute(text("SELECT * FROM [" + str(table_name) + "]"))
        #conn.commit()
      
        #parse columns for header
        columns_raw = str(rows.keys())
        columns_raw = columns_raw[12:]
        columns_raw = columns_raw[:-3]
        columns = columns_raw.split("', '")

        #build header row
        if quoted == True:
          header_row = '"'
        else:
          header_row = ''
        
        for column in columns:
          if quoted == True:
            header_row = header_row + str(column) + '"' + str(seperator) + '"'
          else:
            header_row = header_row + str(column) + str(seperator)

        if quoted == True:
          header_row = header_row[:-2] #remove last quote and seperator
        else:
          header_row = header_row[:-1] #remove last seperator
        	
        #write header row
        csvfile.write(header_row + '\n')
        #write the rest of the rows
        print(0)
        for row in rows:
          writer.writerow(row)
        print(1)

        columns_to_code = columns
      
      rows = None
      for column in columns:
      	
        code = 0

        #print(column)
        with engine.connect() as conn:
          distinct_values = conn.execute(text("SELECT DISTINCT [" + str(column) + "] FROM [" + str(table_name) + "]"))
          #conn.commit()

          unique_values.append([])
          unique_codes.append([])
          
          for distinct_value in distinct_values:
            unique_values[len(unique_values) - 1].append(str(str(distinct_value)[2:-3]))
            unique_codes[len(unique_codes) - 1].append(str(code))
            code = code + 1

      print(server + ".." + database + ".[" + table_name + "] exported as " + str(path) + "\\" + str(table_name) + "." + str(extension))

  print("")

  i = i + 1
  tables_exported_from_sql = tables_exported_from_sql + 1

  ######################
  #CODE THE FILE
  ######################
  #create and export coded file
  df = pd.read_csv(str(path) + "\\" + str(table_name) + "." + str(extension))
  
  col_index = 0
  for column in columns_to_code:
    print(column)
    coded_column = []
    all_values = df[str(column)].values.tolist()
    #input(unique_codes[col_index])
    #input(unique_values[col_index])
    for value in all_values:
        code = unique_codes[col_index][unique_values[col_index].index(str(value))]
        coded_column.append(code)
    #print(coded_column)
    df[str(column) + "__CODED"] = coded_column
    df.to_csv(str(path) + "\\" + str(table_name + "__CODED") + "." + str(extension), index=False)
    col_index = col_index + 1
  #create the key file
  key_file = open(str(path) + "\\" + str(table_name + "__KEY_FILE") + ".csv", "a")
  key_file.write('"COLUMN","VALUE","CODE"')
  
  for column in columns_to_code:
    index = 0
    for unique_value in unique_values:
      key_file.write('"' + str(column) + '","' + '"' + str(unique_value) + '","' + '"' + str(unique_codes[index]) + '","')
      index = index + 1
except Exception as e:
 input(e)	 
  
###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_exported_from_sql) + " " + str(seperator_description) + "-delimited " + str(extension) + " files exported to " + str(path))
print("")

input("Exit the Window")
