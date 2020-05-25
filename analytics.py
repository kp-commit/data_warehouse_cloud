#!/opt/conda/bin/python
"""
analytics.py: Run Analytical Queries
"""
import psycopg2
import loadconfigs as l
from sql_queries import analytical_queries

def analytics(cur, conn):
    print('\nRunning Analytics...')
    for idx,query in enumerate(analytical_queries):    
            print('\nRUNNING QUERY {}:\n{}'.format(idx+1,query))
            cur.execute(query)
            results = cur.fetchall()    
            for row in results:
                print(row)
            conn.commit()
            print('QUERY{} COMPLETED'.format(idx+1))
    print('ANALYTICS COMPLETE')


def main():
    try:
        # Connect to cluster
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            l.DWH_ENDPOINT, l.DWH_DB, l.DWH_DB_USER, l.DWH_DB_PASSWORD, l.DWH_PORT))
        cur = conn.cursor()

        # Run Analytical Queries
        analytics(cur, conn)

        conn.close()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
