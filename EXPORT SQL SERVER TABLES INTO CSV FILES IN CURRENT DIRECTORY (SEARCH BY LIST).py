print("EXPORT SQL SERVER TABLES INTO CSV FILES IN CURRENT DIRECTORY.py")
print("")

###################################################################
#USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04"
database = "TEMP_OK"

#comma-separated, format as "strings", dont include brackets around table name
list_of_table_names = ["GPPO245570_1_EM_EO", "GPPO245570_1_VIA_RAW"] 
#idea: user enters prefix "GPPO245570.1.*", then we find all the tables that match, then we export all of them


#USER INPUT SECTION END
###################################################################






###################################################################
#SET UP
###################################################################
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import os
import csv

engine = create_engine("mssql+pyodbc://:@" + str(server) + "/" + str(database) + "?driver=SQL+Server", use_setinputsizes=False)

###################################################################
#EXPORT
###################################################################
path = os.getcwd() #CURRENT DIRECTORY
i = 0
tables_exported_from_sql = 0

for table_name in list_of_table_names:

  print("table " + str(i + 1) + ": " + str(table_name))

  with engine.connect() as conn:

    with open(str(path) + "\\" + str(table_name) + ".csv", 'w', newline='') as csvfile:
      print(server + ".." + database + ".[" + table_name + "] exporting...")

      writer = csv.writer(csvfile)

      #get the rows from the table
      rows = conn.execute(text("SELECT * FROM [" + str(table_name) + "]"))
    
      #parse columns
      columns_raw = str(rows.keys())
      columns_raw = columns_raw[12:]
      columns_raw = columns_raw[:-3]
      columns = columns_raw.split("', '")

      #build header row
      header_row = ""
      for column in columns:
        header_row = header_row + str(column) + ","
      header_row = header_row[:-1] #remove last comma ,

      #write header row
      csvfile.write(header_row + '\n')

      #write the rest of the rows
      for row in rows:
          writer.writerow(row)

      print(server + ".." + database + ".[" + table_name + "] exported as " + str(path) + "\\" + str(table_name) + ".csv")

    print("")

    i = i + 1
    tables_exported_from_sql = tables_exported_from_sql + 1

    conn.commit()

###################################################################
#FINAL MESSAGE
###################################################################
print("CSV files exported to " + str(path) + ": " + str(tables_exported_from_sql))
print("")

input("Exit the Window")
