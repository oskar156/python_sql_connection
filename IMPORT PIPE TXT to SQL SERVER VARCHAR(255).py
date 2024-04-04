print("IMPORT PIPE TXT to SQL SERVER VARCHAR(255).py")
print("")

###################################################################
# USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04" #example: SQL04
database = "TEMP_OK" #example: TEMP_OK





#USUALLY LEAVE THESE AS IS
qualifier = '"'
seperator = "|"
seperator_description = "pipe"
extension = "txt"

split_file_prefix = "__import_split__"
split_amount = 25000

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

path = os.getcwd() #CURRENT DIRECTORY
	
###################################################################
# IMPORT
###################################################################
engine = create_engine("mssql+pyodbc://:@" + str(server) + "/" + str(database) + "?driver=SQL+Server", use_setinputsizes=False, fast_executemany=True)

i = 0
tables_imported_to_sql = 0
try:
	
 #for every file in the current directory...
 for file in os.listdir(path):

   #open file to import
   filename = os.fsdecode(file)
  
   #if the file does not start with split_file_prefix and it does end with the .extension...
   if not(filename.startswith(str(split_file_prefix))) and filename.endswith("." + str(extension)):  
    
     print("file " + str(i + 1) + ": importing " + str(filename) + "...")

     #get path and name w/o extension
     og_full_path = str(path) + "\\" + str(filename)
     og_filename_no_ext = os.path.splitext(os.path.basename(filename))[0]
     
     #all the split files will share this header_row
     header_row = "" 
        
     #----------
     # SPLIT
     #----------
     print("Splitting into " + str(split_amount) + "-record files...")

     try:

       #below section is from Nav and a little bit from here: https://stackoverflow.com/questions/53028454/splitting-10gb-csv-file-into-equal-parts-without-reading-into-memory
       #input file (the og file)
       with open(file) as infile:
            
         num = 1 # number of current line read
         output_file_num = 1 # number of output file
         header_row_encountered = False
           
         #output file (the split files)
         outfile = open(str(split_file_prefix) + str(og_filename_no_ext) + '-' + str(output_file_num) + "." + str(extension), "w")

         for row in infile:
              
           #if it's the 1st row on the 1st file and we haven't encountered the header row yet
           #then set the header_row, 
           #  and set header_row_encountered and num the 1st split file isn't a record smaller than the rest of the split files and doesn't have 2 header rows
           if header_row_encountered == False and num == 1 and output_file_num == 1:
             header_row = row
             header_row_encountered = True
             num = 0

           #if it's the 1st file
           if output_file_num == 1:
             	
             #if we just encountered the header_row, then write the header_row
             #then num will iterate to 1 on the next loop then we will continue to write rows like usual up to split_amount
             if num == 0:
               outfile.write(header_row)
                 
             #if it's any other row, then j
             elif num >= 1:
               outfile.write(row)
                	
           #if it's any file after the first
           elif output_file_num >= 2:
             	
             #if it's the 1st row, then write the header_row
             if num == 1:
               outfile.write(header_row)
             outfile.write(row)

           # If we got a split_amount rows read, save current out file and create a new one
           if num >= split_amount:
             output_file_num = output_file_num + 1
             outfile.close()
                
             # create new file
             outfile = open(str(split_file_prefix) + str(og_filename_no_ext) + '-' + str(output_file_num) + "." + str(extension), "w")

             # reset counter
             num = 0

           num = num + 1

       # Closing the files
       infile.close()
       outfile.close() 
     
     except Exception as e:
       input(e)
     print("Splitting done.")

     #----------
     # IMPORT
     #----------
     for file in os.listdir(path):
     	
       #open file to import
       filename = os.fsdecode(file)
  
       full_path = str(path) + "\\" + str(filename)

       #if the file does start with split_file_prefix and it does end with the .extension and it isn't empty...
       if filename.startswith(str(split_file_prefix)) and filename.endswith("." + str(extension)) and os.stat(full_path).st_size > 0:  
         print(str(path) + "\\" + str(filename) + " importing... to [" + str(og_filename_no_ext) + "]")    
        
         with engine.connect() as conn:
           #set up table for import, separate by comma ,
           #encoding='latin-1' to avoid the script crashing when running into non-standard characters
           type = defaultdict(lambda: str)
           df = pd.read_csv(full_path, dtype=type, keep_default_na=False, sep=seperator, quotechar=qualifier, encoding='latin-1')
 
           #remove all non-standard characters
           df = df.replace(r'[^A-z0-9\(\)\[\]\{\}`~\.!?@#$%^&\*-_=+\\/|:;\"\',<>\t\ ]', '', regex=True)
           	
           #import table and set each column to NVARCHAR(length=255), no index column, and append if exists
           df.to_sql(og_filename_no_ext , schema='dbo', con=engine, dtype={col_name: NVARCHAR(length=255) for col_name in df}, index=False, if_exists='append')
          	
           #necessary to finalize the import
           conn.commit()

         tables_imported_to_sql = tables_imported_to_sql + 1

     #----------
     # DELETE
     #----------
     print("Deleting the split files...")
     #files_deleted = 0
     try:
       for file in os.listdir(path):
         #open file to import
         filename = os.fsdecode(file)
         
         #if the file does start with split_file_prefix and it does end with the .extension...
         if filename.startswith(str(split_file_prefix)) and filename.endswith("." + str(extension)):  

           if os.path.exists(str(path) + "\\" + str(filename)):
             os.remove(str(path) + "\\" + str(filename))
             #files_deleted = files_deleted+ 1
           else:
             print("The file does not exist")
 
     except Exception as e:
       input(e)
     print("Deleting done.")
     
     print("\n") # LOOPING OG FILE

     i = i + 1

except Exception as e:
 input(e)
 

###################################################################
# FINAL MESSAGE
###################################################################
print(str(i) + " " + str(seperator_description) + "-delimited " + str(extension) + " files imported to " + str(server) + ".." + str(database))
print("")

input("Exit the Window")
