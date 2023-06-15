import psycopg2
import os

def load_data(file_paths):
    """
    Documentation: Load data from files into a PostgreSQL database.

    Args:
        file_paths (list): A list of file paths containing the data.
    """

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        database="processed_shipping",
        user="postgres",
        password="apple123",
        host="localhost",
        port="5432"
    )

    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    for file_path in file_paths:
        # Extract the file name from the file path
        file_name = os.path.basename(file_path)

        # Prepare the query to fetch the layout ID based on the file name
        layout_id_query = f"SELECT layout_id FROM file_pattern_table WHERE '{file_name}' ~ file_pattern;"
        cursor.execute(layout_id_query)
        layout_id = cursor.fetchone()[0]

        # Fetch the header names and data types for the specified layout ID
        header_query = f"SELECT column_name, datatype FROM layout_table1 WHERE layout_id = {layout_id};"
        cursor.execute(header_query)
        columns = cursor.fetchall()

        # Create a temporary table in the database based on the specified layout ID and header names
        temp_table_name = f"temp_layout_{layout_id}"
        create_table_query = f"CREATE TEMP TABLE IF NOT EXISTS {temp_table_name} ("

        # Generate column definitions with data types
        column_definitions = []
        for column in columns:
            column_name, data_type = column
            column_definitions.append(f"{column_name} {data_type}")

        create_table_query += ", ".join(column_definitions)
        create_table_query += ");"

        cursor.execute(create_table_query)

        # Prepare the query to copy data from the file into the temporary table
        copy_query = f"COPY {temp_table_name} FROM STDIN WITH (FORMAT csv, DELIMITER '|', HEADER TRUE);"

        # Open the file and copy its content into the temporary table
        '''The COPY command in PostgreSQL is used to import or export data between files and database tables.
          It provides a fast and efficient way to bulk load or unload large amounts of data.'''
        with open(file_path, 'r') as file:
            cursor.copy_expert(copy_query, file)

        # Check if the 'date_of_birth' column exists in the temporary table
        date_of_birth_exists = 'date_of_birth' in [column[0] for column in columns]

        # If 'date_of_birth' column exists, correct the incorrect year values
        if date_of_birth_exists:
            update_query = f"""
                UPDATE {temp_table_name}
                SET date_of_birth = CASE
                    WHEN EXTRACT(YEAR FROM date_of_birth) >= 2024 AND EXTRACT(YEAR FROM date_of_birth) <= 3000
                    THEN date_of_birth - INTERVAL '100 years'
                    ELSE date_of_birth
                END;
            """
            cursor.execute(update_query)

        # Create the final table based on the layout ID and copy the corrected data from the temporary table
        final_table_name = f"layout_{layout_id}"
        create_final_table_query = f"CREATE TABLE IF NOT EXISTS {final_table_name} AS TABLE {temp_table_name};"
        cursor.execute(create_final_table_query)

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and the connection
    cursor.close()
    conn.close()