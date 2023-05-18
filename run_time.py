import timeit

from script import connect_database, insert_into_postgres

start = timeit.default_timer()
connect_database()
stop = timeit.default_timer()
print('Time taken for connection:', stop -start)

start2 = timeit.default_timer()
insert_into_postgres()
stop2 = timeit.default_timer()
print('Time taken for Data Insertion:', stop2 -start2)


