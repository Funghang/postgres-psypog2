#using regular expression and file identifier
import os
import psycopg2
import logging
from configparser import ConfigParser
import re


def create_table(conn, table_name, column_definitions):
    cur = conn.cursor()
    create_table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({",".join(column_definitions)});'
    cur.execute(create_table_query)
    conn.commit()
    cur.close()


def insert_data(conn, file_path, table_name, line_number):
    cur = conn.cursor()
    with open(file_path, 'r') as file:
        next(file)  # Skip the header line
        line_number = 1
        for line in file: #read the line 
            line_number += 1
            if line.strip():  # Skip empty lines or rows
                try:
                    cur.copy_from(file, table_name, sep='|')
                    conn.commit()
                except psycopg2.Error as e:
                    logging.error(f"Error occurred at line {line_number}: {str(e)}")
                    conn.rollback()
    cur.close()

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
def get_column_data_type(data):
    if data.isdigit():
        return 'INTEGER'
    elif is_float(data):
        return 'DECIMAL'
    else:
        return 'VARCHAR'


def process_file(conn, file_path, table_name, datatype_table):
    with open(file_path, 'r') as file:
        header = file.readline().strip().split('|')
        cur = conn.cursor()
        source_table_query = f"SELECT * FROM datatype_table;"
        cur.execute(source_table_query)
        source_table_columns = cur.fetchall()
        column_definitions = []
        for column in source_table_columns:
            column_name = column[1]
            data_type = column[2]
            if column_name[0].isdigit():
                column_name = f'"{column_name}"'
            column_definitions.append(f"{column_name} {data_type}")
        create_table(conn, table_name, column_definitions)
        insert_data(conn, file_path, table_name, line_number=1)
        cur.close()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    config = ConfigParser()
    config.read("path.config")

    hostname = config.get("postgres", "hostname")
    dbname = config.get("postgres", "database")
    username = config.get("postgres", "username")
    pwd = config.get("postgres", "password")
    port = config.get("postgres", "port")
    _dir = config.get("postgres", "directory")
    tablename = config.get("postgres", "table_name")
    datatype_table = config.get("postgres", "datatype_table")

    conn = psycopg2.connect(
        host=hostname,
        dbname=dbname,
        user=username,
        password=pwd,
        port=port
    )

    try:
        line_number = 1
        pattern = re.compile(r'^files\\.*\.txt$')
        for file_name in os.listdir(_dir):
            file_path = os.path.join(_dir, file_name)
            if pattern.match(file_path):
                
                process_file(conn, file_path, tablename, datatype_table)
                logging.info("Data Inserted!")
    except Exception as e:
        logging.error(f"An error occurred during processing: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()