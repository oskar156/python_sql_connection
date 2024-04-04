print("EXPORT SQL SERVER to FIXED TXT (WITH HEADER AS DEFAULT).py")
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
table_name_re = ""
#--------------------------------------------------------





#USUALLY LEAVE THESE AS IS
add_header = False

seperator_description = "fixed-width"
extension = "txt"

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

###################################################################
# EXPORT
###################################################################
path = os.getcwd() #CURRENT DIRECTORY

i = 0
tables_exported_from_sql = 0

#for every table we found in the last step / for every table the user entered...
for table_name in list_of_table_names:

    print("table " + str(i + 1) + ": " + str(table_name))
    	
    with open(str(path) + "\\" + str(table_name) + "." + str(extension), 'w', newline='') as csvfile:
    	
      print(server + ".." + database + ".[" + table_name + "] exporting...")

      #create file object
      writer = csv.writer(csvfile)

      #get columns
      with engine.connect() as conn:
        headers = conn.execute(text("SELECT TOP 0 * FROM [" + str(table_name) + "]"))
        
        #parse columns for header
        columns_raw = str(headers.keys())
        columns_raw = columns_raw[12:]
        columns_raw = columns_raw[:-3]
        columns = columns_raw.split("', '")
        conn.commit()

      #get max lenghts of each column and add 1 to each so each field can have a space between them
      max_lens = []
      with engine.connect() as conn:

        for column in columns:
          max_length_raw = conn.execute(text("SELECT MAX(LEN([" + str(column) + "])) FROM [" + str(table_name) + "]"))    
          
          max_lens_raw = []
          for row in max_length_raw:
            max_lens_raw.append(row)

          max_length = int(max_lens_raw[0][0]) + 1
          
          if add_header == True and len(column) + 1 > max_length:
            max_lens.append(len(column) + 1)
          else:
            max_lens.append(max_length)
              
          conn.commit()

      if add_header == True:
        #write header row and add spaces as needed
        column_index = 0
        for column in columns:
          number_of_spaces_needed = max_lens[column_index] - len(str(column))
          n = 0
          while n < number_of_spaces_needed:
            column = str(column) + " "
            n = n + 1
          
          csvfile.write(column)
        
          column_index = column_index + 1
        
        csvfile.write('\n')
        
      #write each row and add spaces as needed
      with engine.connect() as conn:
    
        rows = conn.execute(text("SELECT * FROM [" + str(table_name) + "]"))
        
        #for each row in the table
        for row in rows:

          #remove open and closing ()
          row = str(row)[1:]
          row = str(row)[:-1]

          #if there are ' at the start and end of the row, remove them
          #check for " just in case too
          if str(row)[0] == "'" or str(row)[0] == "\"":
            row = str(row)[1:]
          if str(row)[-1] == "'" or str(row)[-1] == "\"":
            row = str(row)[:-1]

          #if a ' exists in the cell, then it will bound with "" instead of the usual
          #this makes sure all cells are seperated by ', ' so it splits correctly
          #this will break if ', ' is in the data!
          row = row.replace("', \"",  "', '")
          row = row.replace("\", '",  "', '")
          row = row.replace("\", \"", "', '")

          #if it's a single column of data, then there'll be a ', at the end of the row, so remove it
          #just in case, check for ", as well
          if str(row)[-2:] == "'," or str(row)[-2] == "\",":
            row = str(row)[:-2] 

          fields = str(row).split("', '")
          
          column_index = 0

          #for each field in a row
          for field in fields:
            field = field.strip()
            
            #calculate number of spaces that need to be added to this field and add them
            number_of_spaces_needed = max_lens[column_index] - len(str(field))
            n = 0
            while n < number_of_spaces_needed:
              field = str(field) + " "
              n = n + 1

            csvfile.write(field)
            column_index = column_index + 1
            
          csvfile.write('\n')
       
        conn.commit()
        
        
    #create column-spec file
    column_spec_file = open(str(path) + "\\" + str(table_name) + "_column_specs.txt", 'w', newline='')
    
    #column-spec header
    column_spec_header = '"COLUMNS","START","END"\n'
    column_spec_file.write(column_spec_header)
    column_spec_file.flush()
    
    #column-spec rows (each column in the fw txt file should be a row in the column-spec file)
    current_char_index = 0
    column_index = 0
    
    for column in columns:
      start_index = current_char_index
      end_index = start_index + max_lens[column_index]
      current_char_index = end_index
      column_spec_row = '"' + str(column) + '","' + str(start_index) + '","' + str(end_index) + '"\n'
      column_spec_file.write(column_spec_row)
      column_spec_file.flush()
      
      column_index = column_index + 1
      
    column_spec_file.close()
    
    print("")

    i = i + 1
    tables_exported_from_sql = tables_exported_from_sql + 1
    



###################################################################
# FINAL MESSAGE
###################################################################
print(str(tables_exported_from_sql) + " " + str(seperator_description) + " " + str(extension) + " files exported to " + str(path) + '\n')
print("Column-spec file for each fixed-width txt file also created (0-indexed).")
print("")

input("Exit the Window")
