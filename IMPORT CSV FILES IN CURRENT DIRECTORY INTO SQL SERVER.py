print("IMPORT CSV FILES IN CURRENT DIRECTORY INTO SQL SERVER.py")
print("")

###################################################################
#USER INPUT SECTION, UPDATE AS NEEDED

server = "SQL04"
database = "TEMP_OK"

#USER INPUT SECTION END
###################################################################






###################################################################
#SET UP
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
#IMPORT
###################################################################
path = os.getcwd() #CURRENT DIRECTORY

i = 0
tables_imported_to_sql = 0

for file in os.listdir(path):

    filename = os.fsdecode(file)
    print("file " + str(i + 1) + ": " + str(filename))

    if filename.endswith(".csv"): 
        print(str(path) + "\\" + str(filename) + " importing...")

        full_path = str(path) + "\\" + str(filename)
        filename_no_ext = os.path.splitext(os.path.basename(filename))[0]

        with engine.connect() as conn:

          type = defaultdict(lambda: str)
          df = pd.read_csv(full_path, dtype=type, keep_default_na=False)
          df.to_sql(filename_no_ext , schema='dbo', con=engine, dtype={col_name: NVARCHAR(length=255) for col_name in df}, index=False, if_exists='append')
          conn.commit()

        print(str(path) + "\\" + str(filename) + " imported as [" + str(filename_no_ext) + "]")

        tables_imported_to_sql = tables_imported_to_sql + 1

    else:
        print(str(path) + "\\" + str(filename) + " skipped because it's not a csv file.")

    print("")
    i = i + 1

###################################################################
#FINAL MESSAGE
###################################################################
print("tables imported to " + str(server) + ".." + str(database) + ": " + str(tables_imported_to_sql))
print("")

input("Exit the Window")
