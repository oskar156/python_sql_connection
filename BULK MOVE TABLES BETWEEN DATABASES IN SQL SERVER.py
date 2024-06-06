print("BULK MOVE TABLES BETWEEN DATABASES FROM SQL SERVER.py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04" #example: SQL04
database = "TEMP_OK" #example: TEMP_OK
destination_database = "TEMP_BMW" #example: TEMP_OK

#There are 2 ways to search for tables to export...
# IF OPTION 1 IS FILLED OUT, OPTION 1 WILL BE USED
# IF OPTION 1 IS KEPT = [], OPTION 2 WILL BE USED

#--------------------------------------------------------
# OPTION 1 - SEARCH BY LIST:
#format as a list of strings ["table1", "table2", "etc..."]
list_of_table_names = [""]
#--------------------------------------------------------

#--------------------------------------------------------
# OPTION 2 - SEARCH BY REGEX:
#REGEX TEMPLATE TO GET ALL OF AN ORDER'S TABLES IF THEY'RE USING dash - or underscore _
#just copy it between the quotes after table_name_re = and replace order numbers as needed
#GPPO243510[-_](1$|1[-_])
table_name_re = "GPPO246456"

#--------------------------------------------------------



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

input("Press enter to drop the tables found above.\nOr exit the script window to cancel.\n...")
print("")

###################################################################
# MOVE
###################################################################

i = 0
tables_moved = 0

#for every table we found in the last step / for every table the user entered...
for table_name in list_of_table_names:

  print("table " + str(i + 1) + ": " + str(table_name))

  with engine.connect() as conn:

    try:
      print(server + "." + database + "..[" + table_name + "] moving...")
      conn.execute(text("SELECT * INTO " + str(destination_database) + "..[" + str(table_name) + "] FROM " + str(database) + "..[" + str(table_name) + "]" ))
      print(server + "." + database + "..[" + table_name + "] moved")
      print("")
    except Exception as e:
      input(e)
    i = i + 1
    tables_moved = tables_moved + 1

    conn.commit()

###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_moved) + " tables moved from " + str(server) + "." + str(database) + " to " + str(server) + "."+ str(destination_database))
print("")

input("Exit the Window")
