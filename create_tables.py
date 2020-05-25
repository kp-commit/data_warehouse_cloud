#!/opt/conda/bin/python
"""
create_tables.py: Drop & Create tables in Redshift
- Drop all tables
- Create Stagging, Fact & Dim Tables
"""
import psycopg2
import loadconfigs as l
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        

def main():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        l.DWH_ENDPOINT, l.DWH_DB, l.DWH_DB_USER, l.DWH_DB_PASSWORD, l.DWH_PORT))
    cur = conn.cursor()

    print('Droping Tables in Cluster...', end='')
    drop_tables(cur, conn)
    print('Dropped!')

    print('Creating Tables in Cluster...', end='')
    create_tables(cur, conn)
    print('Created!')

    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
