#!/opt/conda/bin/python
"""
sql_queries.py: All SQL queries used for DROP, CREATE, INSERT, SELECT COUNT()
- Drop tables
- Create Staging, Fact & Dim Tables
- Insert into Staging from S3, then into Fact & Dim Tables
- Count rows inserts in all tables
"""

# Load all Config variables
import loadconfigs as l

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS ft_songplays"
user_table_drop = "DROP TABLE IF EXISTS dm_users"
song_table_drop = "DROP TABLE IF EXISTS dm_songs"
artist_table_drop = "DROP TABLE IF EXISTS dm_artists"
time_table_drop = "DROP TABLE IF EXISTS dm_time"

# CREATE TABLES
staging_events_table_create = ("""
	CREATE TABLE IF NOT EXISTS staging_events (
		artist VARCHAR,
		auth VARCHAR,
		firstName VARCHAR,
		gender CHAR(1),
		itemInSession INTEGER,
		lastName VARCHAR,
		length FLOAT,
		level VARCHAR(10),
		location VARCHAR,
		method VARCHAR,
		page VARCHAR,
		registration FLOAT,
		sessionId INTEGER,
		song VARCHAR,
		status INTEGER,
		ts TIMESTAMP SORTKEY,
		userAgent VARCHAR,
		userId INTEGER
	);
""")

staging_songs_table_create = ("""
	CREATE TABLE IF NOT EXISTS staging_songs (
		num_songs INTEGER,
		artist_id VARCHAR,
		artist_latitude DECIMAL(10,6),
		artist_longitude DECIMAL(10,6),
		artist_location VARCHAR,
		artist_name VARCHAR,
		song_id VARCHAR,
		title VARCHAR,
		duration DECIMAL,
		year INTEGER
	);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS ft_songplays (
		songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
		start_time TIMESTAMP NOT NULL,
		user_id INTEGER NOT NULL,
		level VARCHAR(10),
		song_id VARCHAR NOT NULL,
		artist_id VARCHAR NOT NULL DISTKEY SORTKEY,
		session_id INTEGER,
		location VARCHAR,
		user_agent VARCHAR
	);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dm_users (
	    user_id INTEGER NOT NULL PRIMARY KEY DISTKEY,
	    first_name VARCHAR NOT NULL,
	    last_name VARCHAR NOT NULL,
	    gender CHAR(1) NOT NULL,
	    level VARCHAR(10) NOT NULL
	);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dm_songs (
	    song_id VARCHAR NOT NULL PRIMARY KEY DISTKEY,
	    title VARCHAR NOT NULL,
	    artist_id VARCHAR NOT NULL SORTKEY,
	    year INTEGER NOT NULL,
	    duration DECIMAL
	);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dm_artists (
	    artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY DISTKEY,
	    name VARCHAR NOT NULL,
	    location VARCHAR,
	    latitude DECIMAL(10,6),
	    longitude DECIMAL(10,6)
	);
""")

time_table_create = ("""
	CREATE TABLE IF NOT EXISTS dm_time (
		start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
		hour INTEGER NOT NULL,
		day INTEGER NOT NULL,
		week INTEGER NOT NULL,
		month INTEGER NOT NULL,
		year INTEGER NOT NULL,
		weekday INTEGER NOT NULL
	);
""")

# COPY TO STAGING TABLES FROM S3
staging_events_copy = ("""
	COPY {} FROM '{}' 
	IAM_ROLE '{}'
	TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
	TIMEFORMAT AS 'epochmillisecs'
	JSON '{}'
	COMPUPDATE OFF
	REGION 'us-west-2';
""").format('staging_events', l.LOG_DATA, l.DWH_ROLE_ARN, l.LOG_JSONPATH)

staging_songs_copy = ("""
	COPY {} FROM '{}' 
	IAM_ROLE '{}'
	TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
	JSON 'auto'
	COMPUPDATE OFF
	REGION 'us-west-2';
""").format('staging_songs', l.SONG_DATA, l.DWH_ROLE_ARN)


# FINAL TABLES
user_table_insert = ("""
	INSERT INTO dm_users (user_id, first_name, last_Name, gender, level)
	SELECT DISTINCT (userId) AS user_id, 
		firstName AS first_name, 
		lastName AS last_name, 
		gender AS gender, 
		level AS level
	FROM staging_events
	WHERE userId IS NOT NULL
	AND page = 'NextSong';
""")

song_table_insert = ("""
	INSERT INTO dm_songs (song_id, title, artist_id, year, duration)
	SELECT DISTINCT (song_id), 
		title,
		artist_id,
		year,
		duration
	FROM staging_songs
	WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
	INSERT INTO dm_artists (artist_id, name, location, latitude, longitude)
	SELECT DISTINCT (artist_id) AS artist_id,
		artist_name AS name,
		artist_location AS location,
		artist_latitude AS latitude,
		artist_longitude AS longitude
	FROM staging_songs
	WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
	INSERT INTO dm_time (start_time, hour, day, week, month, year, weekday)
	SELECT DISTINCT (ts),
		EXTRACT(hour FROM ts),
		EXTRACT(day FROM ts),
		EXTRACT(week FROM ts),
		EXTRACT(month FROM ts),
		EXTRACT(year FROM ts),
		EXTRACT(weekday FROM ts)
	FROM staging_events
	WHERE ts IS NOT NULL
	AND userId IS NOT NULL
	AND page = 'NextSong';
""")

songplay_table_insert = ("""
	INSERT INTO ft_songplays (start_time, user_id, level, song_id, artist_id,
							  session_id, location, user_agent)
	SELECT DISTINCT (se.ts),
		se.userId,
		se.level,
		ss.song_id,
		ss.artist_id,
		se.sessionId,
		se.location,
		se.userAgent
	FROM staging_events se
	JOIN staging_songs ss
	ON se.song = ss.title
	AND se.artist = ss.artist_name
	WHERE se.userId IS NOT NULL
	AND se.page = 'NextSong';
""")

# COUNT QUERIES
staging_events_table_count  = (""" 
	SELECT 'Staging_Events:', COUNT(*) FROM staging_events;
""")
staging_songs_table_count  = (""" 
	SELECT 'Staging_Songs:', COUNT(*) FROM staging_songs;
""")
songplay_table_count  = (""" 
	SELECT 'Songplays:', COUNT(*) FROM ft_songplays;
""")
user_table_count  = (""" 
	SELECT 'Users:', COUNT(*) FROM dm_users;
""")
song_table_count  = (""" 
	SELECT 'Songs:', COUNT(*) FROM dm_songs;
""")
artist_table_count  = (""" 
	SELECT 'Artists:', COUNT(*) FROM dm_artists;
""")
time_table_count  = ("""
	SELECT 'Time:', COUNT(*) FROM dm_time;
""")


# ANALYTICAL QUERIES
# Top 10 Songs
top_songs = ("""
    WITH topsid as (
    	SELECT song_id, count(*)
    	FROM ft_songplays
    	GROUP BY song_id
    	ORDER BY 2 DESC
    	LIMIT 10
    )
    SELECT '"'|| dms.title ||'" by '|| dma.name || ' played ' || ft.count || ' times.'
    FROM  topsid as ft
    INNER JOIN dm_songs dms 
    ON ft.song_id = dms.song_id
    INNER JOIN dm_artists dma
    ON dms.artist_id = dma.artist_id
    GROUP BY dms.title, dma.name, ft.count
    ORDER BY ft.count DESC
""")


# Top 10 Artists
top_artists = ("""
    WITH topart as (
    	SELECT artist_id, song_id, count(*)
    	FROM ft_songplays
    	GROUP BY artist_id, song_id
    	ORDER BY 2 DESC
    	LIMIT 10
    )
    SELECT '"'||dma.name||'" was played '||ft.count||' times.'
    FROM  topart as ft
    INNER JOIN dm_artists dma 
    ON ft.artist_id = dma.artist_id
    INNER JOIN dm_songs dms 
    ON ft.song_id = dms.song_id
    GROUP BY dma.name, ft.count
    ORDER BY ft.count DESC
""")

# Paid versus Free Ratio
paid_free_rt = ("""
    WITH p as (
    	SELECT count(user_id) as paid 
    	FROM dm_users
    	WHERE level = 'paid'
    ),
    f as (
    	SELECT count(user_id) as free
    	FROM dm_users
    	WHERE level = 'free'
    )
    SELECT 'Ratio: ', 'Free users: '||f.free, 'Paid users: '||p.paid, (f.free/p.paid)||':'||(p.paid/p.paid)
    FROM p,f
""")

# Peek Usage day of week
peek_usage_day = ("""
    SELECT 
    	CASE 
    		WHEN dmt.weekday = 1 THEN 'Monday'
    		WHEN dmt.weekday = 2 THEN 'Tuesday' 
    		WHEN dmt.weekday = 3 THEN 'Wednesday' 
    		WHEN dmt.weekday = 4 THEN 'Thursday' 
    		WHEN dmt.weekday = 5 THEN 'Friday'
    		WHEN dmt.weekday = 6 THEN 'Saturday' 
    		WHEN dmt.weekday = 7 THEN 'Sunday'
    	END,
    	'Total songplays:', count(*) 
    FROM dm_time dmt
    GROUP BY dmt.weekday
    ORDER BY 2 DESC
    LIMIT 1
""")


# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert, songplay_table_insert]

count_queries = [staging_events_table_count, staging_songs_table_count,
                 songplay_table_count, user_table_count, song_table_count,
                 artist_table_count, time_table_count]

analytical_queries = [top_songs, top_artists, paid_free_rt, peek_usage_day]
