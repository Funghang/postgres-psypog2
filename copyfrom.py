import psycopg2

hostname = 'localhost'
database = 'processed_shipping'
username = 'postgres'
pwd = 'apple123'
port_id = 5432
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()
    
    create_variable = '''CREATE TABLE IF NOT EXISTS shipping_data(
    RECORD_ID varchar(20),
    RX_NUMBER varchar(35),
    SHIPPING_ADDRESS_1 varchar(25),
    SHIPPING_ADDRESS_2 varchar(26),
    SHIPPING_CITY char(20),
    SHIPPING_STATE char(21),
    SHIPPING_ZIP_CODE varchar(22),
    SHIPPING_CARRIER char(19),
    SHIPPING_TRACKING_NUMBER varchar(40),
    SHIPPING_TYPES varchar(29))'''

    cur.execute(create_variable)
    conn.commit()
    
except Exception as error:
    print(error)
        
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

# Assuming you have the file 'processed_shipping.txt' in the current directory
# filename = 'processed_shipping.txt'

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()
#    with open('processed_shipping.txt', 'r') as file:
#       lines = file.readlines()

#    for line in lines:
#       values = line.strip().split('|')
#            print(values)
#        cur.execute("INSERT INTO shipping_data (RECORD_ID, RX_NUMBER, SHIPPING_ADDRESS_1, SHIPPING_ADDRESS_2, SHIPPING_CITY, SHIPPING_STATE, SHIPPING_ZIP_CODE, SHIPPING_CARRIER, SHIPPING_TRACKING_NUMBER, SHIPPING_TYPES) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
#                      (values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9]))
    cur.execute("copy shipping_data(RECORD_ID, RX_NUMBER, SHIPPING_ADDRESS_1, SHIPPING_ADDRESS_2, SHIPPING_CITY, SHIPPING_STATE, SHIPPING_ZIP_CODE, SHIPPING_CARRIER, SHIPPING_TRACKING_NUMBER, SHIPPING_TYPES) from 'processed_shipping.txt' DELIMITER '|' csv header");
    conn.commit()
    
except Exception as error:
    print(error)
        
finally:
    if cur is not None:
        cur.close() 
    if conn is not None:
        conn.close()
