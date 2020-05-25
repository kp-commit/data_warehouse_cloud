#!/opt/conda/bin/python
"""
cluster_connect.py: Sample Adhoc query execution script
- Load Staging Tables
- Insert into Fact & Dim Tables
- Count Rows Inserted
"""
import psycopg2
import loadconfigs as l


# Connect to cluster
conn_string="postgresql://{}:{}@{}:{}/{}".format(l.DWH_DB_USER, 
             l.DWH_DB_PASSWORD, l.DWH_ENDPOINT, l.DWH_PORT, l.DWH_DB)
conn = psycopg2.connect(conn_string)

# Set auto commit on
conn.set_session(autocommit=True)

# Open a cursor
cur = conn.cursor()

# Execute a query, simply uncomment and run.

cur.execute("""            
    SELECT
    	tablename
    FROM
    	pg_catalog.pg_tables
    WHERE
    	schemaname != 'pg_catalog'
    AND schemaname != 'information_schema';
""")
    

#--------------------- SAMPLE QUERIES: -----------------------#

## Get Tablenames in schema
#cur.execute("""            
#    SELECT
#    	tablename
#    FROM
#    	pg_catalog.pg_tables
#    WHERE
#    	schemaname != 'pg_catalog'
#    AND schemaname != 'information_schema';
#""")
#results = cur.fetchall()
#
#

## Get columsn in given table    
#cur.execute("""     
#   SELECT
#    	COLUMN_NAME
#    FROM
#    	information_schema.COLUMNS
#    WHERE
#    	TABLE_NAME = 'ft_songplays';
##""")


## Check Load errors:
#cur.execute("""  
#    SELECT * 
#    FROM stl_load_errors;
#""")
#    
#
    
    
## Get Row Counts in each table
# cur.execute("""
#	SELECT 'Staging_Events:', COUNT(*) FROM staging_events;
#	SELECT 'Staging_Songs:', COUNT(*) FROM staging_songs;
#	SELECT 'Songplays:', COUNT(*) FROM ft_songplays;
#	SELECT 'Users:', COUNT(*) FROM dm_users;
#	SELECT 'Songs:', COUNT(*) FROM dm_songs;
#	SELECT 'Artists:', COUNT(*) FROM dm_artists;
#	SELECT 'Time:', COUNT(*) FROM dm_time;
#"""
#)
# ------------------------------------------------------------#



# Fetch results
results = cur.fetchall()

# Display results
for row in results:
    print(row)