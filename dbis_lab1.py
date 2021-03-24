import psycopg2
import sys
import csv
import re
import time
import codecs
import os

from psycopg2 import sql
from psycopg2._psycopg import OperationalError

file_name, dbname, user, password, host, port = sys.argv

pattern_mark = re.compile(r'^\d+,\d+$')  # шаблон для поиска чисел с запятой в данных
pattern_file = re.compile(r'\d{4}')  # шаблон для поиска года в названии файла

data_files = ['Odata2020File.csv', 'Odata2019File.csv']   # название файла с данными
counter_file = 'string_number.txt' # название файл с счетчиком


def connection_true(dbname, user, password, host, port):
    conn = None
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    except:
        print("Connection error")
    return conn

def execute_query(query, cursor, connection):
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        connection.close()
        sys.exit()

def execute_read_query(query, connection):
    result = []
    try:
        with connection.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
        return result
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        connection.close()
        sys.exit()

def create_table(connection):
    if not os.path.exists(counter_file):
        with open(counter_file, "w") as counter:
            counter.write("0")
    with open("create_table.sql", 'r') as create:
        res = ''.join([row for row in create])
        with connection.cursor() as cur:
            execute_query(res, cur)
        connection.commit()

def database_task(connection):
    with codecs.open("database_task.sql", 'r', encoding="utf-8") as task:
        res = ''.join([row for row in task])
        result = execute_read_query(res, connection)
        with open("query_result.csv", "w") as query_result:
            for row in result:
                query_result.write(",".join([str(elem) for elem in row])+"\n")
        #for row in result:
        #    print(row)

conn = connection_true(dbname, user, password, host, port)
if conn is None:
    sys.exit()

cursor = conn.cursor()
create_table(conn)
for data_file in data_files:
    with open(data_file, newline='', encoding="cp1251") as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        head = next(reader)
        head.insert(0, "year")
        data_year = re.findall(pattern_file, data_file)[0]
        with open("string_number.txt", "r") as f1:
            result = int(f1.read())
            for n, row in enumerate(reader):
                row.insert(0, data_year)
                try:
                    if n >= result:
                        for b, i in enumerate(row):
                            if re.match(pattern_mark, i):
                                row[b] = row[b].replace(",", ".")
                            if i == "null":
                                row[b] = None
                        insert = sql.SQL('INSERT INTO ZNO_DATA ({}) VALUES ({})').format(
                            sql.SQL(',').join([sql.Identifier(i.lower()) for i in head]),
                            sql.SQL(',').join(map(sql.Literal, row))
                        )
                        cursor.execute(insert)
                        if (n+1) % 100 == 0 and n > 0:
                            conn.commit()
                            with open(counter_file, "w") as f:
                               f.write(str(n+1))

                        print(n)
                        time.sleep(0.15)
                except (psycopg2.ProgrammingError, psycopg2.IntegrityError, psycopg2.DataError) as e:
                    conn.rollback()
                    if conn is not None:
                        conn.close()
                    print(f"Inserts errors = {e}")
                    sys.exit()
                except (psycopg2.OperationalError, psycopg2.DatabaseError, psycopg2.InterfaceError) as e:
                    print(f"Connections errors = {e}")
                    sys.exit()
os.remove(counter_file)
database_task(conn)
conn.close()
