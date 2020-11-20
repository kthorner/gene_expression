#!/usr/local/Python-3.7/bin/python
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import pymysql
import getpass
import sys

data = pd.read_csv("BRCAcellLines.csv", sep =',') # Get file
filtered = data[data.gene != "---"]
filtered_dd = filtered.drop_duplicates(subset=['gene'])
gene_list = filtered['gene'] # Get only the gene column
gene_list_dd = gene_list.drop_duplicates()
cell_list = pd.DataFrame({'cell':filtered.columns[1:]})
print("Genes and cells filtered from file")

# Output processed data
with open('BRCA_cells.txt', 'w') as csv_file:
	cell_list.to_csv(path_or_buf=csv_file, chunksize = 2000, index = False, header = False)
	
with open('BRCA_genes.txt', 'w') as csv_file:
	gene_list_dd.to_csv(path_or_buf=csv_file, chunksize = 2000, index = False, header = False)
	
cell_query = ''' SELECT clid, cl_name
				FROM cell_line JOIN temp ON cell_line.cl_name = temp.value'''
				
gene_query = ''' SELECT gid, gene_name
				FROM gene JOIN temp ON gene.gene_name = temp.value'''

connection = pymysql.connect(user="kthorner", password=getpass.getpass(prompt='Enter password for MySQL: ', stream=None), database="groupI", port=4253, local_infile=True)
with connection.cursor() as cursor:
	query1 = '''LOAD DATA LOCAL INFILE "BRCA_cells.txt" INTO TABLE temp (value);'''
	query2 = '''LOAD DATA LOCAL INFILE "BRCA_genes.txt" INTO TABLE temp (value);'''
	query3 = '''TRUNCATE temp;'''
	query4 = '''LOAD DATA LOCAL INFILE "BRCA_express.txt" 
				INTO TABLE gene_expression 
				FIELDS TERMINATED BY ',' 
				LINES TERMINATED BY '\n' 
				(gid, clid, did, expression);'''
	cursor.execute(query1)
	connection.commit()
	cell_ids = pd.read_sql(cell_query, connection)
	cursor.execute(query3)
	connection.commit()
	cursor.execute(query2)
	connection.commit()
	gene_ids = pd.read_sql(gene_query, connection)
	cursor.execute(query3)
	connection.commit()
	
	print("gid and clid retrieved from database")

	gene_df = pd.DataFrame(gene_list_dd)
	gene_merge = gene_df.merge(gene_ids, left_on = 'gene', right_on = 'gene_name')
	gene_merge = gene_merge.drop('gene',1)
	gene_merge = gene_merge.drop('gene_name',1)
	matrix = pd.DataFrame(columns=['gid','clid','did','ex'])
	for index, row in cell_list.iterrows(): 
		table = gene_merge.copy()
		cell = row['cell']
		clid = int(cell_ids.loc[cell_ids['cl_name'] == cell, 'clid'])
		table.insert(1, 'clid', clid)
		table.insert(2, 'did', 1)
		table.insert(3, 'ex', filtered_dd.loc[:,cell].values)
		matrix = pd.concat([matrix, table], ignore_index = True)
	
	matrix = matrix.round({'ex': 5})	
	print("Expression matrix generated")
		
	with open('BRCA_express.txt', 'w') as csv_file:
		matrix.to_csv(path_or_buf=csv_file, chunksize = 20000, index = False, header = False)	

	cursor.execute(query4)
	connection.commit()
	
	print("Successfully imported into table")
	
cursor.close()
connection.close()
