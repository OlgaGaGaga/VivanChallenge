# VivanChallenge

LookOnData.ipynb file was used to explore and clean data and find suitable placeholders for NaN values. Also, some functions for data processing were tested here first. \
main.py file has functions are needed for reading data, creating a database ,and loading data:
1. connect_db(in_host, in_user, in_passwd, in_db) - connecting database using host name, user name, password and database name\
2. create_db(name, cursor) - creating database; database name and cursor. You can make a cursor using:\
`mydb = connect_db('host', 'user', 'password', 'Vivan')`\
`mycursor = mydb.cursor())`
3. read_process_json(file) - function processes benchling_entries.json. It parses json file, extracts all the info for genes to be up/down regulated, and split this table to table to be up and table to be down regulated genes. 2 final tables are up_parsed.json and down_parsed.json.
4. read_cnv(file) - a very simple function to read a cnv file to pandas dataframe
5. fast_insert_data_in_db(connection, cursor, db_name, table_name, dict_col_type, pd_df) - final version for data insertion function. Pass it connection, cursor, name of database and a table to be created, dictionary with column names as keys and column types as values: 
`dict_col_type_up = {'patID': 'VARCHAR(255)', 'upHsgene': 'VARCHAR(255)', 'upDmgene': 'VARCHAR(255)', 'upDELDUPL': 'VARCHAR(255)', 'upConfidence': 'VARCHAR(255)', 'upComments': 'VARCHAR(255)'}`, and pandas dataframe to be inserted into database

parsed_json.csv is a version of an unsplit parsed json file. However, it is not aligned with the "tidy data" conception.
SQL_queries.sql has all the queries to process uploaded data (e.g. to replace NaN placeholders with NULL) and then to extract information requested in the text of the task.

The database is constructed from 3 tables: copies, downGenes, and upGenes, made from corresponded files: cnv_processed_nan_processed.txt, down_parsed_json.csv, and up_parsed_json.csv.
