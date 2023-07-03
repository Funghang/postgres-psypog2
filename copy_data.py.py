#program to copy data from .txt to postgres using laytout_id, files table  where the file path of processed_demographics.txt is given

import psycopg2
import os

def establish_connection():
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        database="processed_shipping",
        user="postgres",
        password="apple123",
        host="localhost",
        port="5432"
    )
    return conn

def close_connection(conn):
    # Close the connection
    conn.close()

def retrieve_files(conn, file_path):
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    # Extract the file name from the file path
    file_name = os.path.basename(file_path)

    # Prepare the query to fetch the layout ID based on the file name
    layout_id_query = f"SELECT layout_id FROM files WHERE '{file_name}' ~ file_pattern;"
    cursor.execute(layout_id_query)
    layout_id = cursor.fetchone()[0]

    # Fetch the header names and data types for the specified layout ID
    header_query = f"SELECT column_name, datatype FROM layout_table WHERE layout_id = {layout_id};"
    cursor.execute(header_query)
    columns = cursor.fetchall()

    # Create the table based on the layout ID and header names
    create_table_query = f"CREATE TABLE IF NOT EXISTS layout_{layout_id} ("

    # Generate column definitions with data types
    column_definitions = []
    for column in columns:
        column_name, data_type = column
        column_definitions.append(f"{column_name} {data_type}")

    create_table_query += ", ".join(column_definitions)
    create_table_query += ");"

    cursor.execute(create_table_query)

    # Prepare the query to copy data from the file into the table
    copy_query = f"COPY layout_{layout_id} FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER TRUE);"

    # Open the file and copy its content into the table
    with open(file_path, 'r') as file:
        cursor.copy_expert(copy_query, file)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cursor.close()

def main():
    # File path
    file_path = r'C:\Users\DELL\Desktop\postgres\processed_demographics.txt'

    # Establish the connection
    conn = establish_connection()

    # Retrieve file and match the pattern
    retrieve_files(conn, file_path)

    # Close the connection
    close_connection(conn)

# Execute the main function
if __name__ == "__main__":
    main()