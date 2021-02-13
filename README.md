# Project description
As an evolution of [Project 2](https://github.com/hedcler/udacity-dataengineer-project2) Sparkify has grown their user base and song database and want to move their processes and data onto the cloud.

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I was tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

**1. Create infrastructure [OPTIONAL]**

**2. Design and create schemas**

**3. Build ETL Pipeline**

## Project structure
We have a file called `sql_queries.py` with all SQL Queries used in the project, a file called `create_tables.py` that start/restart the database structure on postgres, and a file called `etl.py` that execute the process of read all files and migrate their data in a strucured way for our database.

**Optionally** we can use the file `iac.py` tha will create AWS needed infrastructure, if you don't do it yet. This file will also update the configuration file with the cluster variables.


## Step 1 - The ETL process



## Step 2 - Reading the data



**QUERIES**

