print("BULK DROP TABLES FROM SQL SERVER.py")
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
#--------------------------------------------------------

#--------------------------------------------------------
# OPTION 2 - SEARCH BY REGEX:
#REGEX TEMPLATE TO GET ALL OF AN ORDER'S TABLES IF THEY'RE USING dash - or underscore _
#just copy it between the quotes after table_name_re = and replace order numbers as needed
#GPPO243510[-_](1$|1[-_])
table_name_re = "_OLD$"

"""
_OLD$ - removes all tables that end with _OLD (dedupe)
"""
#--------------------------------------------------------





#USUALLY LEAVE THESE AS IS
quoted = True

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
# DROP
###################################################################

i = 0
tables_dropped_from_sql = 0

#for every table we found in the last step / for every table the user entered...
for table_name in list_of_table_names:

  print("table " + str(i + 1) + ": " + str(table_name))

  with engine.connect() as conn:

    try:
      print(server + "." + database + "..[" + table_name + "] dropping...")
      conn.execute(text("DROP TABLE " + str(database) + "..[" + str(table_name) + "]"))
      print(server + "." + database + "..[" + table_name + "] dropped")
      print("")
    except Exception as e:
      input(e)
    i = i + 1
    tables_dropped_from_sql = tables_dropped_from_sql + 1

    conn.commit()

###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_dropped_from_sql) + " tables dropped")
print("")

input("Exit the Window")
