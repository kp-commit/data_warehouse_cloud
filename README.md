# Cloud Data Warehouse on AWS
#### using Boto3, S3, IAM, ETL, Redshift, Pandas, Python
---

### Contents
    - Business Requirement
    - Solution
    - Datasets
    - Steps taken
    - Table Design
    - File Contents and Purpose of each file
    - How to Run the Python Scripts
    - Sample Execution
    - Common Issues and Resolution

---
### Business Requirement:
- A music streaming company, Sparkify has grown their user base and song database.
- Now, they **want to move their processes and data onto the cloud.** 
- Their data resides in **S3**, 
- in a directory of **JSON logs on user activity** on the app, 
- as well as a directory with **JSON metadata** on the songs in their app.  

### Solution:
- **Data engineering** is required for building an **ETL pipeline** that _extracts_ their data from **S3**, 
- _stages_ them in **Redshift**, and _transforms_ data into a set of _dimensional tables_ for their
- **analytics team** to continue finding _insights_ in what songs their users are listening to. 
- Afterwards, check the database and _ETL pipeline_ by running _queries_ given to you by the analytics team.


### Datasets:
---

Working with two datasets that reside in S3. Here are the S3 links for each: 

1. Song data: ```s3://udacity-dend/song_data ```
2. Log data: ```s3://udacity-dend/log_data```, Log data JSON metadata: ```s3://udacity-dend/log_json_path.json``` 

**Song Dataset**

The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset. 

```
song_data/A/B/C/TRABCEI128F424C983.json 
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
Log


```


**Log Dataset**

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) with activity logs from a music streaming app based on specified configurations. The log files in the dataset are partitioned by year and month.

For example, here are filepaths to two files in this dataset. 
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```


And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

```
{"artist":"Blue October \/ Imogen Heap","auth":"Logged In","firstName":"Kaylee","gender":"F","itemInSession":7,"lastName":"Summers","length":241.3971,"level":"free","location":"Phoenix-Mesa-Scottsdale, AZ","method":"PUT","page":"NextSong","registration":1540344794796.0,"sessionId":139,"song":"Congratulations","status":200,"ts":1541107493796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"","userId":"8"}
```

![2018-11-12-events.json](/images/json.png)

###  

### Steps taken:
---
#### Setup AWS
1. Setup AWS and update creditinals in config
    - Manually setup an AWS account, obtain key and secret 
    - Set these in configuration file ```dwh.cfg```

#### Create Cluster
2. Build Python script ```create_cluster.py``` to connect to **AWS** and create a **Redshift Cluster**:
    - Use ```Boto3``` to create ```iam```, ```redshift``` clients  
    - Create ```iam role``` for cluster, obtain ```RoleArn```
    - Create a new cluster, obtain ```endpoint``` url
    - Set RoleArn and endpoint in ```dwh.cfg```
    
#### Create Table Schemas
3. Build an ETL pipeline for a **Data Warehosue** hosted on **Redshift Cluster.**
    - Create Table Schemas: Design schemas for your fact and dimension tables.
    - Initiated by ```create_tables.py``` running ```sql_queries.py``` for ```CREATE```, ```DROP```
    - Launch a redshift cluster and create an IAM role that has read access to S3.
    - Add redshift database and IAM role info to ```dwh.cfg```  

#### Load data to Staging Tables
4. Load data from **S3 to staging tables on Redshift**
    - Implement the logic in ```etl.py``` to load data from S3 to staging tables on Redshift.
    - Take care of Duplicates, Transformations and other constraints as needed.
    - Run quick check on database for table counts after insertion. Referenced within calls to ```sql_queries.py```

#### Run Analytics
5. Execute ```analytics.py``` that runs **SQL** queries that analytics team provided to finding _insights_ in what songs their users are listening to. 

#### Delete Cluster 
6. Build ```cluster_delete.py``` that deletes the Redshift cluster. 

###  

### Table Design:
---

### Staging Table: staging_events
##### Note: Columns named to match Log data json file
##### Lowest possible column size taken as possible to maximize storage and processing performace
##### No other major constraints are these are temporary staging tables
```
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
```

### Staging Table: staging_songs
##### Note: Columns named to match song data json files
##### Lowest possible column size taken as possible to maximize storage and processing performance
##### No other major constraints are these are temporary staging tables
```
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
```

### Dimension Table: dm_users
##### Lowest possible column size taken as possible to maximize storage and processing performance
##### Assigned DISTKEY on user_id both utilized in many joins as later seen in analytical queries
```
user_id INTEGER NOT NULL PRIMARY KEY DISTKEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender CHAR(1) NOT NULL,
level VARCHAR(10) NOT NULL
```
### Dimension Table: dm_songs
##### Note: Assigned DISTKEY on title because of high number of entries (this is the biggest table in set). 
##### SORTKEY on song_id because of ORDER BY on this to minimize shuffling.
```
song_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
title VARCHAR NOT NULL DISTKEY,
artist_id VARCHAR NOT NULL,
year INTEGER NOT NULL,
duration DECIMAL
```

### Dimension Table: dm_artists
##### Note: Assigned DISTKEY on title because of high number of entries
##### DISTKEY on name because of number of groupings done on this analytics and this is also 3rd biggest table in set.
```
artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
name VARCHAR NOT NULL DISTKEY,
location VARCHAR,
latitude DECIMAL(10,6),
longitude DECIMAL(10,6)
```
### Dimension Table: dm_time
##### Note: Assigned DISTKEY on start_time because similiar start_time can be easily looked up.
##### SORTKEY on weekday because of analytics looking into simliar weekday

```
start_time TIMESTAMP NOT NULL PRIMARY KEY DISTKEY,
hour INTEGER NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday INTEGER NOT NULL SORTKEY
```

### Fact Table 
##### Note: Assigned DISTKEY and SORTKEY on song_id it is utilized in many joins for lookups and also for ORDER BY
```
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR(10),
song_id VARCHAR NOT NULL DISTKEY SORTKEY,
artist_id VARCHAR NOT NULL,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR
```


###  


### File Contents and Purpose of each file
---
- ```README.md``` - Readme file that includes summary, how to run the Python scripts, and an explanation of all the files
- ```env.sh``` - Bash script to update environment PATH and make all .py files executeable
- ``` dwh.cfg``` - Configuration file with sections for AWS, DWH (Redshift Cluster), S3. Lists their parameters with values.
- ```loadconfigs.py``` - Python script to load and write all configuration params
- ```cluster_create.py``` - Python script to create a Redshift Cluster
- ```cluster_status.py``` - Python script to check status of Redshift Cluster and get endpoint
- ```cluster_connect.py``` - Python script to check status of Redshift Cluster connection and run Ad-hoc queries
- ```create_tables.py``` - Python script to DROP any previously created tables and CREATE Redshift tables (Staging and Final Dimensions and Fact Tables) referencing queries in ```sql_queries.py```
- ```sqlqueries.py``` - Contains all SQL queries used through all Python scripts
- ```etl.py``` - Python script to Perform ETL operations and load final data into final tables for analysis 
- ```analytics.py``` - Python script to execute all Analytical queries to find insights
- ```cluster_delete.py``` - Python script to delete current Redshift Cluster


###  


### How to Run the Python Scripts:
---
1. **Open a terminal** and change to scripts location e.g. /home/workspace
2. **Modify permissions** to make ```env.sh``` executeable:
    ```chmod +x env.sh``` and execute ```./env.sh```
3. **Execute cluster_create.py**: ```./cluster_create.py```
4. **Execute create_tables.py**: ```./create_tables.py```
5. **Execute etl.py**: ```./etl.py``` and check table count outputs.
6. **Execute analytics.py**: ```./analytics.py``` and check query outputs.
7. **Optionally**, to **delete cluster** execute cluster_delete.py: ```./cluster_delete.py```


###  

### Sample Execution:
---
#### 1. Open a terminal and change to scripts location e.g. /home/workspace
![New Terminal Window](/images/new_terminal.png)

#### 2. Modify permissions to make ```env.sh``` executeable:
```
root@d5ef17e2c407:/home/workspace# ./env.sh 
PATH updated
/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/workspace
*.py files made executeable
root@d5ef17e2c407:/home/workspace# 
```

#### 3. Execute ```./cluster_create.py```
```
root@d5ef17e2c407:/home/workspace# ./cluster_create.py 
Creating a new IAM Role...An error occurred (EntityAlreadyExists) when calling the CreateRole operation: Role with name dwhRole already exists.
Attaching Policy to IAM Role...Done
IAM role ARN: arn:aws:iam::264307067173:role/dwhRole
Creating Cluster...in progress...
                 Key         Value
0  ClusterIdentifier  dwhcluster  
1  NodeType           dc2.large   
2  ClusterStatus      creating    
3  MasterUsername     dwhuser     
4  DBName             dwh         
5  VpcId              vpc-544d202c
6  NumberOfNodes      2           
Cluster creating, waiting for endpoint...####################################################################################################
Opening TCP port in VPC
ec2.SecurityGroup(id='sg-d0e1738e')
An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule "peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW" already exists
Endpoint AVAILABLE: dwhcluster.ciuwmycyuglg.us-west-2.redshift.amazonaws.com
```

#### 4. Execute ```./create_tables.py```
```
root@d5ef17e2c407:/home/workspace# ./create_tables.py 
Droping Tables in Cluster...Dropped!
Creating Tables in Cluster...Created!
```


#### 5. Execute ```./etl.py``` and check table count outputs.
```
root@d5ef17e2c407:/home/workspace# ./etl.py 
Load Staging Tables...
QUERY1 COMPLETED
QUERY2 COMPLETED
LOADED

Insert into Fact & Dim Tables
QUERY1 COMPLETED
QUERY2 COMPLETED
QUERY3 COMPLETED
QUERY4 COMPLETED
QUERY5 COMPLETED
INSERTS COMPLETED

Count Rows Inserted
Staging_Events: 8056
Staging_Songs: 14896
Songplays: 333
Users: 104
Songs: 14896
Artists: 10025
Time: 6813
COUNTS ROWS COMPLETED
```

#### 6. Execute ```./analytics.py``` and check query outputs.
```
root@d5ef17e2c407:/home/workspace# ./analytics.py 

Running Analytics...

RUNNING QUERY 1:

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

('"You\'re The One" by Dwight Yoakam played 37 times.',)
('"Catch You Baby (Steve Pitron & Max Sanna Radio Edit)" by Lonnie Gordon played 9 times.',)
('"I CAN\'T GET STARTED" by Ron Carter played 9 times.',)
('"Nothin\' On You [feat. Bruno Mars] (Album Version)" by B.o.B played 8 times.',)
('"Hey Daddy (Daddy\'s Home)" by Usher featuring Jermaine Dupri played 6 times.',)
('"Hey Daddy (Daddy\'s Home)" by Usher played 6 times.',)
('"Make Her Say" by Kid Cudi / Kanye West / Common played 5 times.',)
('"Up Up & Away" by Kid Cudi / Kanye West / Common played 5 times.',)
('"Up Up & Away" by Kid Cudi played 5 times.',)
('"Make Her Say" by Kid Cudi played 5 times.',)
('"Mr. Jones" by Counting Crows played 4 times.',)
('"Unwell (Album Version)" by matchbox twenty played 4 times.',)
('"Supermassive Black Hole (Album Version)" by Muse played 4 times.',)
QUERY1 COMPLETED

RUNNING QUERY 2:

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

('"Steppenwolf" was played 3 times.',)
('"The Airborne Toxic Event" was played 1 times.',)
('"Staind" was played 1 times.',)
('"Tego Calderón" was played 1 times.',)
('"Elena" was played 1 times.',)
('"Tego" was played 1 times.',)
('"Aphex Twin" was played 1 times.',)
('"The Format" was played 1 times.',)
('"Dead Kennedys" was played 1 times.',)
('"The Waterboys" was played 1 times.',)
('"Polygon Window" was played 1 times.',)
('"Tego Calderon" was played 1 times.',)
('"Pigeon John" was played 1 times.',)
QUERY2 COMPLETED

RUNNING QUERY 3:

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

('Ratio: ', 'Free users: 82', 'Paid users: 22', '3:1')
QUERY3 COMPLETED

RUNNING QUERY 4:

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

('Saturday', 'Total songplays:', 627)
QUERY4 COMPLETED
ANALYTICS COMPLETE
```

#### 7. Optionally, to delete cluster execute ```./cluster_delete.py```:
```
root@d5ef17e2c407:/home/workspace# ./cluster_delete.py 
Detaching IAM Role
Deleting Cluster
                 Key                                                                                  Value
0  ClusterIdentifier  dwhcluster                                                                           
1  NodeType           dc2.large                                                                            
2  ClusterStatus      deleting                                                                             
3  MasterUsername     dwhuser                                                                              
4  DBName             dwh                                                                                  
5  Endpoint           {'Address': 'dwhcluster.ciuwmxxcyuglg.us-west-2.redshift.amazonaws.com', 'Port': 5439}
6  VpcId              vpc-544d202c                                                                         
7  NumberOfNodes      2                                                                                    

Deleting Cluster...###########################An error occurred (ClusterNotFound) when calling the DescribeClusters operation: Cluster dwhcluster not found.
DELETED. Cluster not present now.
```

###  

### Common Issues and Resolution:

#### 1. Load errors seen through load process
![Load_errors_into_Redshift_from_S3](/images/Load_errors_into_Redshift_from_S3.png)

Resolution: Check ```stl_load_errors``` table for details.
![Load_errors_check4](/images/Load_errors_check4.png)

Found an issue with timestamp earlier during development, addressed with help of stl_load_errors and fixed column to hold epoch properly.
AWS documentation on epoch:
![Load_errors_check4_resolution](/images/Load_errors_check4_resolution.png)

Address by change in COPY statment with epoch:
![Load_errors_check4_resolution1](/images/Load_errors_check4_resolution1.png)


#### 2. AWS errors
![AWS_error](/images/AWS_error.png)
Resolution: Found incorrect COPY statement with creditinals. Addressed with correct usage and resolved.

#### 3. Duplicates
![Analytics_error1_duplicates2](/images/Analytics_error1_duplicates2.png)

Resolution: Modified query for additional grouping sets:
![Analytics_error1_duplicates4](/images/Analytics_error1_duplicates4.png)


#### 4. Python script execution error in Terminal (bad interpreter^M)
![cluster_status_bad_interpreter_error_cluster_status](/images/cluster_status_bad_interpreter_error_cluster_status.png)
File contained extra encoding for carriage returns '\r'.

Resolution: used tr to remove extra encoding:
```tr -d '\r' < filename.py > newfile.py && mv newfile.py filename.py && chmod +x filename.py```