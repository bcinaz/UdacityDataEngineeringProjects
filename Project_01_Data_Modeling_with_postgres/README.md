# Project1 Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## How to run
```
%run create_tables
%run etl
```

## Data Model
The sparkify database design uses the simple star schema. The schema contains one fact table: songplays, and four dimension tables: songs, artists, users and time. The tables are depicted below:


**Table songplays:**

start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

| Columns | Description |
| --- | --- |
| songplay_id | serial (primary key) |
| start_time | timestamp |
| user_id | integer |
| level | varchar |
| song_id | varchar |
| artist_id | varchar |
| sessio_id | int |
| location | varchar |
| user_agent | varchar |

**Table songs:**

| Columns | Description |
| --- | --- |
| song_id | varchar (primary key) |
| title | varchar |
| artist_id | varchar |
| year | int |
| duration | numeric |

**Table artists:**

| Columns | Description |
| --- | --- |
| artist_id | varchar (primary key) |
| name | varchar |
| location | varchar |
| latitude | decimal |
| longitude | decimal |

**Table users:**

| Columns | Description |
| --- | --- |
| user_id | int (primary key) |
| first_name | varchar |
| last_name | varchar |
| gender | varchar |
| level | varchar |

**Table time:**

| Columns | Description |
| --- | --- |
| start_time | timestamp (primary key) |
| hour | int |
| day | int |
| week | int |
| month | int |
| year | int |
| weekday | int |

## Files in the repository
* sql_queries.py : describes all the necessary SQL queries needed during data processing.
* create_tables.py : creates the database, connects to the database, drops all the tables (if exist), creates all tables needed and at the end closes the connection to the database. 
* etl.py : reads and processes files from song_data and log_data and loads them into created tables.
* etl.ipynb : this notebook contains detailed instructions on the ETL process for each of the tables.
* test.ipynb : this notebook is used to test if tables are created and if the values are inserted correctly.

## ETL Process
The whole ETL pipeline works as follows: 
* Connection to the database is created.
* Song files are processed by extracting the information on songs and artists and inserting these values to songs and artists tables.
* Log files are processed by extracting the time information (in datetime format) and user information and inserting these values to corresponding tables. Additionally, other information from the songs and artists table are read and aggregated with the information extracted from log files and the aggregated values are inserted into songplays table. 

