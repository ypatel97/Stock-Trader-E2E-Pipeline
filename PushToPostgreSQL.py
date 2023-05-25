# ----- Use this if you want to publish data to postgreSQL database and then retrieve and format the data using Spark
# ----- This will produce the same file that is produced by format_stored_data() but it will push the data to
# ----- to a local PostgreSQL server then retrieve and format that data using Apache Spark
# *******  MAKE SURE TO RUN THE API TO GET THE DATA BEFORE RUNNING THIS *******
import os
import psycopg2
import csv
from pyspark.sql import SparkSession

def push_data_to_postgreSQL():

    # Database connection details
    host = 'localhost'
    database = 'Stock Pipeline'
    user = 'postgres'
    password = '########'
    port_id = 5432
    conn = None
    cursor = None

    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(host=host,
                                database=database,
                                user=user,
                                password=password,
                                port=port_id)

        cursor = conn.cursor()

        # List of CSV files and corresponding table names

        csv_files = get_files()

        # Iterate over CSV files and upload to tables
        for csv_file in csv_files:
            file_path = csv_file['file']
            table_name = csv_file['table']

            # Read CSV files as DataFrame using Spark
            spark = SparkSession.builder.appName('CSV Reader').getOrCreate()
            df = spark.read.csv(file_path, header=True, inferSchema=True)

            # Create the table if it doesn't exist
            create_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            -- Define your column names and data types here
                            column1 datatype,
                            column2 datatype,
                            column3 datatype
                        )
                    """
            cursor.execute(create_table_query)
            conn.commit()

            # Open the CSV file and execute the COPY command to import data
            with open(file_path, 'r') as file:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", file)

            # Commit the changes
            conn.commit()
            print(f"Data from {file_path} uploaded to {table_name}")

    except Exception as error:
        print(error)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def get_files():

    # Folder path
    folder_path = 'data'

    csv_files = []

    # Iterate over files in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        # Check if the current path is a file
        if os.path.isfile(file_path):
            file = {'file': f'{file_path}', 'table': filename[:-4]}
            csv_files.append(file)

    return csv_files

def format_postgreSQL_data():
    pass


if __name__ == '__main__':
    push_data_to_postgreSQL()
    format_postgreSQL_data()