import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import time
from os.path import isfile
from pathlib import Path
from configparser import ConfigParser
import shutil

# Need to break up this program into Functions and Classes and Test Before Main Document Export

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

# import sqlscanneddocs

parser = ConfigParser()
parser.read(r'C:\Users\VCarrigan\Config\mtairy-docs.ini')

mtairy_docs_path = parser.get('mtairy_variables', 'mtairy_docs_path')
mtairy_docs_output_path = parser.get('mtairy_variables', 'mtairy_docs_output_path')
mtairy_colonoscopy_path = parser.get('mtairy_variables', 'mtairy_colonoscopy_path')
mtairy_echo_path = parser.get('mtairy_variables', 'mtairy_echo_path')
mtairy_ekg_path = parser.get('mtairy_variables', 'mtairy_ekg_path')
mtairy_eye_path = parser.get('mtairy_variables', 'mtairy_eye_path')
mtairy_mammogram_path = parser.get('mtairy_variables', 'mtairy_mammogram_path')
mtairy_pap_path = parser.get('mtairy_variables', 'mtairy_pap_path')
conn_prep = parser.get('mtairy_variables', 'conn_prep')
conn_land = parser.get('mtairy_variables', 'conn_land')

print('Starting')
print('Relevant Directories are:')
print(mtairy_docs_path)

output_directories = [mtairy_colonoscopy_path, mtairy_echo_path,mtairy_ekg_path,mtairy_eye_path,mtairy_mammogram_path,
                      mtairy_pap_path]

# Clear Output Directories for Troubleshooting: Make more efficient by using a list of directories
def clear_output_directories():
    print('Clearning Output Directories')
    for directory in output_directories:
        for filename in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, filename)):
                os.remove(os.path.join(directory, filename))

clear_output_directories()

# Database Connections
quoted_land = quote_plus(conn_land)
land_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted_land)
engine_land = create_engine(land_con)
connection = engine_land.raw_connection()
cursor = connection.cursor()

document_list = [f for f in os.listdir(mtairy_docs_path) if isfile(os.path.join(mtairy_docs_path, f))]
sql_truncate = "truncate table mtairy.files"
cursor.execute(sql_truncate)

sql = ('''INSERT INTO mtairy.files (full_file_name,
                                    chart_number,
                                    patient_last, 
                                    patient_first, 
                                    patient_middle, 
                                    patient_dob, 
                                    document_date, 
                                    document_type, 
                                    document_title, 
                                    file_number
    ) VALUES (?,?,?,?,?,?,?,?,?,?)''')  # Use ? place holder for good practice

split_list = []
# Attempt to deal with a single row first
for d in document_list:
   split_list.clear()
   split_list.append(d.split('_',8))
   single_row = d.split('_',8)
   cursor.execute(sql, d, single_row[0], single_row[1], single_row[2], single_row[3], single_row[4], single_row[5],
                  single_row[6], single_row[7], single_row[8])

print('Loading File Name Information into LAND.mtairy.files')
cursor.commit()
cursor.close()


# Run the Procedure to Load the Data Into the PREP Database
quoted_prep = quote_plus(conn_prep)
prep_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted_prep)
engine_prep = create_engine(prep_con)
connection = engine_prep.raw_connection()
cursor = connection.cursor()

sql_load_prep = 'execute mtairy.load_files'
cursor.execute(sql_load_prep)
cursor.commit()
print('Loading File Name Information into PREP.mtairy.files')
print('Settng Export Categories for Files')
cursor.execute('execute mtairy.update_files')
cursor.commit()
cursor.close()

########################################################################################################################

# This section will select the most recent item from each category, then copy that file to the right directory
connection = engine_prep.raw_connection()
cursor = connection.cursor()

def copy_most_recent(category):

    sql_query = '''SELECT full_file_name 
                 FROM PREP.mtairy.files f 
                 WHERE f.export_category in (?)
                 AND f.seq = 1'''

    results = cursor.execute(sql_query, category)
    result_list = []

    for row in results:   # Row objects are returned by the query results
        result_list.append(str(row))

    formatted_list = []

    for r in result_list:
        file_name = str(r)
        file_name = file_name.replace("'","")
        file_name = file_name.lstrip("(")
        file_name = file_name.rstrip(")")
        file_name = file_name.rstrip(",")
        # file_name = file_name.replace(",", "")
        # file_name = file_name.replace("(","")
        formatted_list.append(file_name)

    for f in formatted_list:  #This is for visualy seeing the file names and what they contain
        print(f)
        print(type(f))

    for f in formatted_list:
        if category == 'Colonoscopy':
            shutil.copy(os.path.join(mtairy_docs_path, f), os.path.join(mtairy_colonoscopy_path, f))
        elif category == 'Echo':
            shutil.copy(os.path.join(mtairy_docs_path, f), os.path.join(mtairy_echo_path, f))
        elif category == 'EKG':
            shutil.copy(os.path.join(mtairy_docs_path, f), os.path.join(mtairy_ekg_path, f))
        elif category == 'Eye Exam':
            shutil.copy(os.path.join(mtairy_docs_path, f), os.path.join(mtairy_eye_path, f))
        elif category == 'Mammogram':
            shutil.copy(os.path.join(mtairy_docs_path, f), os.path.join(mtairy_mammogram_path, f))
        elif category == 'Pap':
            shutil.copy(os.path.join(mtairy_docs_path, f), os.path.join(mtairy_pap_path, f))

copy_most_recent('Colonoscopy')
copy_most_recent('Echo')
copy_most_recent('EKG')
copy_most_recent('Eye Exam')
copy_most_recent('Mammogram')
copy_most_recent('Pap')


