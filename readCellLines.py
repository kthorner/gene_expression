#!/usr/local/Python-3.7/bin/python
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import pymysql
import getpass
import sys

data = pd.read_csv("BRCASampleAnnotations.csv", sep =',') # Get file
data.loc[data['PIK3CA Mutation'].notnull(), 'PIK3CA Mutation'] = 1
data.loc[data['PIK3CA Mutation'].isnull(), 'PIK3CA Mutation'] = 0
data.columns = ['name','sub_value','type','mut_value','file']
data.insert(1, 'subtype', 'claudin3')
data.insert(4, 'mutation', 'PIK3CA')

# Output processed data
with open('cell_lines.txt', 'w') as csv_file:
	data.to_csv(path_or_buf=csv_file, chunksize = 2000, index = False, header = False)


connection = pymysql.connect(user="kthorner", password=getpass.getpass(prompt='Password: ', stream=None), database="groupI", port=4253, local_infile=True)
with connection.cursor() as cursor:
	query = '''LOAD DATA LOCAL INFILE "cell_lines.txt" INTO TABLE cell_line
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n' 
	(cl_name,subtype,subtype_value,cl_type,mutation,mutation_value,file_name);'''
	cursor.execute(query)

cursor.close()
connection.commit()
connection.close()
