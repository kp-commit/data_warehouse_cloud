#!/opt/conda/bin/python
"""
etl.py: Load data into Redshift tables and display counts
- Load Staging Tables
- Insert into Fact & Dim Tables
- Count Rows Inserted
"""
import psycopg2
import loadconfigs as l
from sql_queries import copy_table_queries, insert_table_queries, count_queries


def load_staging_tables(cur, conn):
    print('\nLoad Staging Tables...')
    for idx,query in enumerate(copy_table_queries):    
            cur.execute(query)
            conn.commit()
            print('QUERY{} COMPLETED'.format(idx+1))
    print('LOADED')


def insert_tables(cur, conn):
    print('\nInsert into Fact & Dim Tables')
    for idx,query in enumerate(insert_table_queries):
        cur.execute(query)
        conn.commit()
        print('QUERY{} COMPLETED'.format(idx+1))
    print('INSERTS COMPLETED')


def count_check(cur, conn):
    print('\nCount Rows Inserted')
    for idx,query in enumerate(count_queries):
        cur.execute(query)
        results = cur.fetchall()    
        for row in results:
            print(row[0],row[1])
        conn.commit()
    print('COUNTS ROWS COMPLETED')


def main():
    try:
        # Connect to cluster
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            l.DWH_ENDPOINT, l.DWH_DB, l.DWH_DB_USER, l.DWH_DB_PASSWORD, l.DWH_PORT))
        cur = conn.cursor()

        # Load Staging Tables
        load_staging_tables(cur, conn)
        
        # Insert into Fact & Dim Tables
        insert_tables(cur, conn)
        
        # Count Rows Inserted
        count_check(cur, conn)

        conn.close()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
