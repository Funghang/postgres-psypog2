import psycopg2
from configparser import ConfigParser

from query import create_table, insert_data, truncate_table


def connect_database():
    config = ConfigParser()
    config.read("cred.config")

    hostname = config.get("postgres", "hostname")
    dbname = config.get("postgres", "database")
    username = config.get("postgres", "username")
    pwd = config.get("postgres","password")
    port = config.get("postgres", "port")

    conn = psycopg2.connect(
        host=hostname,
        dbname=dbname,
        user=username,
        password=pwd,
        port=port
    )
    return conn

def insert_into_postgres():
    conn = connect_database()
    cur = conn.cursor()

    try:
        cur.execute(create_table)
        conn.commit()

        cur.execute(truncate_table)
        conn.commit()

        with open ('processed_shipping.txt','r') as file:
            lines = file.readlines()
            
            for line in lines:
                line = line.strip().split('|')
            
                cur.execute(insert_data, (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9]))
            conn.commit()
    except Exception as error:
        print(error)

    finally:
        cur.close()
        conn.close()   

def main():
    insert_into_postgres()

if __name__ == "__main__":
    main()
