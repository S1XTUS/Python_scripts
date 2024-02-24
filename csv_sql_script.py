import pandas as pd
import os
import pyodbc

# Connection details
server = 'Kartik'
database = 'F1_new'
username = 'sa'
password = 'Kk@12345'
conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Path to directory containing CSV files
csv_directory = r"E:\Kartik\Data Science\F1\cleaned_data_files"
# Establish connection to SQL Server
try:
    conn = pyodbc.connect(conn_str)
    print("Connection to database made")
except pyodbc.Error as e:
    print(f"Error connecting to database: {e}")
    exit()

cursor = conn.cursor()

# Function to create table and load data from CSV file
def create_table_and_load_data(csv_file_path, cursor):
    # Extract filename from path
    filename = os.path.splitext(os.path.basename(csv_file_path))[0]

    # Read the CSV file
    try:
        df = pd.read_csv(csv_file_path, quotechar='"')
        print(f"File '{csv_file_path}' read successfully")
    except Exception as e:
        print(f"Error reading file '{csv_file_path}': {e}")
        return

    # Mapping Pandas data types to SQL Server-compatible data types
    sql_data_types = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'VARCHAR(MAX)',
        'datetime64': 'DATETIME'
    }

    # Construct CREATE TABLE statement dynamically
    columns_str = ', '.join([f'{col} {sql_data_types[str(df[col].dtype)]}' for col in df.columns])
    create_table_sql = f'CREATE TABLE {filename} ({columns_str})'

    # Execute CREATE TABLE statement
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table '{filename}' created successfully")
    except pyodbc.Error as e:
        print(f"Error creating table '{filename}': {e}")
        return

    # Execute BULK INSERT statement to load data into the newly created table
    bulk_insert_sql = f'''
    BULK INSERT {filename}
    FROM '{csv_file_path}'
    WITH (
        FIELDTERMINATOR = ',',  -- Assuming comma-separated values
        ROWTERMINATOR = '\n',    -- Assuming newline-delimited rows
        FIRSTROW = 2             -- Skip header row if applicable
    )
    '''
    try:
        cursor.execute(bulk_insert_sql)
        conn.commit()
        print(f"Data from '{csv_file_path}' inserted into table '{filename}' successfully")
    except pyodbc.Error as e:
        print(f"Error inserting data into table '{filename}': {e}")

# Process each CSV file in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        csv_file_path = os.path.join(csv_directory, filename)
        create_table_and_load_data(csv_file_path, cursor)

# Close connections
cursor.close()
conn.close()
