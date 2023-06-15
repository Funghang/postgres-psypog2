#using subprocess.run()
import csv
from decimal import InvalidOperation
import subprocess
import psycopg2 
import os

from configparser import ConfigParser


def create_table_and_insert_data(path, table_name, conn):
    with open(path, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        data = next(reader)

    cur = conn.cursor()
    column_defination = ','.join(f'{col}{get_column_data_type(data)}' for col, data in zip(header, data))
    create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({column_defination});'
    cur.execute(create_table_query)
    conn.commit()

    psql_command = f'psql -d database_name - U user -c "COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER \', \');"'
    subprocess.run(psql_command, input=open(file, 'r'), shell=True)
    cur.close()

def get_column_data_type(data):
    try:
        int(data)
        return 'INTEGER'
    except ValueError:
        try:
            float(data)
            return 'DECIMAL'
        except (ValueError, InvalidOperation):
            return 'VARCHAR'
    
def process_new_files(dir, conn, command):
    processed_file = set()

    inotify_command = f'inotifywait -m -e create -e moved_to --format "%w%f" {dir}'
    inotify_process = subprocess.Popen(inotify_command, stdout=subprocess.PIPE, shell=True)

    while True:
        new_file = inotify_process.stdout.readline().readline().decode().strip()
        _, file_ext = os.path.splitext(new_file)

        if file_ext == '.csv' and new_file not in processed_file:
            create_table_and_insert_data(new_file, os.path.splitext(os.path.basename(new_file))[0], conn)
            process_new_file(new_file, command)
            processed_file.add(new_file)

    inotify_process.terminate()

def process_new_file(file_path, script_path):
    subprocess.run(['bash',script_path, file_path], check=True)

def main():
    config = ConfigParser()
    config.read("cred.config")

    hostname = config.get("postgres", "hostname")
    dbname = config.get("postgres", "database")
    username = config.get("postgres", "username")
    pwd = config.get("postgres","password")
    port = config.get("postgres", "port")      
    _dir = config.get("postgres", "directory")
    command = "process_script.sh"

    conn = psycopg2.connect(
        host=hostname,
        dbname=dbname,
        user=username,
        password=pwd,
        port=port
    )

    try:
        process_new_files(_dir, conn, command)
    finally:
        conn.close()

if __name__ == "__main__":  
    main()