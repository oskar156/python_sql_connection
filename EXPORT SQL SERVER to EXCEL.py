print("EXPORT SQL SERVER to EXCEL.py")
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
list_of_table_names = []
#--------------------------------------------------------

#--------------------------------------------------------
# OPTION 2 - SEARCH BY REGEX:
#REGEX TEMPLATE TO GET ALL OF AN ORDER'S TABLES IF THEY'RE USING dash - or underscore _
#just copy it between the quotes after table_name_re = and replace order numbers as needed
#GPPO243510[-_](1$|1[-_])
table_name_re = ""
#--------------------------------------------------------





#USUALLY LEAVE THESE AS IS
extension = "xlsx"
combine_exported_tables_into_single_workbook = False #default is False
combined_table_name = "" #if combine_exported_tables_into_single_workbook is True, then this must be filled out

# USER INPUT SECTION END
###################################################################





###################################################################
# SET UP
###################################################################
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import inspect
import pandas as pd
from pandas.io.formats import excel
import os
import re

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
excel.ExcelFormatter.header_style = None #removes the header formatting df.to_excel() has by default

i = 0
tables_exported_from_sql = 0

#for every table we found in the last step / for every table the user entered...

#IF WE ARE EXPORTING 1 EXCEL FILE FOR EACH TABLE
if combine_exported_tables_into_single_workbook == False: 
	
  for table_name in list_of_table_names:

    print("table " + str(i + 1) + ": " + str(table_name))
  	
    writer = pd.ExcelWriter(str(table_name) + '.xlsx')
  
    with engine.connect() as conn:

      print(server + ".." + database + ".[" + table_name + "] exporting...")
    
      rows = conn.execute(text("SELECT * FROM [" + str(table_name) + "]"))
      df = pd.DataFrame(list(rows))
      df.to_excel(writer, sheet_name=table_name, index=False)
      writer.close()

      print(server + ".." + database + ".[" + table_name + "] exported as " + str(path) + "\\" + str(table_name) + "." + str(extension))
      print("")

      i = i + 1
      tables_exported_from_sql = tables_exported_from_sql + 1
      conn.commit()
      
  print(str(tables_exported_from_sql) + " " +  str(extension) + " excel files exported to " + str(path))
  print("")   
   
#IF WE ARE EXPORTING 1 EXCEL FILE TOTAL WITH EACH TABLE AS A NEW TAB IN THE SAME EXCEL FILE
else: 

  writer = pd.ExcelWriter(str(combined_table_name) + '.xlsx')

  for table_name in list_of_table_names:

    print("table " + str(i + 1) + ": " + str(table_name))
  	
    with engine.connect() as conn:

      print(server + ".." + database + ".[" + table_name + "] exporting...")
    
      rows = conn.execute(text("SELECT * FROM [" + str(table_name) + "]"))
      df = pd.DataFrame(list(rows))
      df.to_excel(writer, sheet_name=table_name, index=False)
      

      print(server + ".." + database + ".[" + table_name + "] exported as " + str(path) + "\\" + str(combined_table_name) + "." + str(extension) + " (tab: " + str(table_name) + ")")
      print("")

      i = i + 1
      tables_exported_from_sql = tables_exported_from_sql + 1
      conn.commit()
      
  writer.close()

  print("1 xlsx excel file with " + str(tables_exported_from_sql) + " tabs exported to " + str(path))
  print("")  

###################################################################
# FINAL MESSAGE
###################################################################
input("Exit the Window")
