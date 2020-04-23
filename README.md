# Data Engineering Nanodegree Program

## Introduction
In this program, I have learned to design data models, build data warehouses and data lakes, automate data pipelines, and work with massive datasets. 

## Projects
1. [Data Modelling with PostgreSQL](https://github.com/jason-huynh83/Udacity-Data-Engineering-Projects/tree/master/Data_Modeling_PostgreSQL)
-   **Completed Tasks:**
    -   Create a relational database using PostgreSQL
    -   Developed a star schema optimized for queries on the fact & dimension tables
    -   Implemented ETL (Extract, Transform, Load) pipeline to create and load data into fact and dimension tables.
-   **Concepts Learned:**
    -   Normalization
    -   ACID Principle
    -   Star & Snowflake Schema
    -   ETL Workflows
-   **Core Technologies Used:**
    -   Python (Pandas, Jupyter, psycopg2)
    -   PosgreSQL
2. [Data Modeling Apache Cassandra](https://github.com/jason-huynh83/Udacity-Data-Engineering-Projects/tree/master/Data_Modeling_Apache_Cassandra)
-   **Completed Tasks:**    
    -   Created a NoSQL Database with Apache Cassandra
    -   Created Tables in Keyspace based on defined queries that denormalizes the star schema, optimized to answer business questions
    -   Implemented ETL pipeline to create and load data into tables
-   **Concepts Learned:**
    -   Distribute Database Design
    -   CAP(Consistency, Availability, Partition Tolerance) Theorem
    -   Partitioning with Primary Key & Clustering Columns
-   **Core Technologies Used:** 
    -   Python (Pandas, Jupyter, cassandra)
    -   Apache Cassandra
3. [Cloud Data Warehouse](https://github.com/jason-huynh83/Udacity-Data-Engineering-Projects/tree/master/Cloud_Data_Warehouse)
-   **Completed Tasks:**
    -   Created a Redshift Cluster on AWS(Created roles & users)
    -   Staged raw data from S3 into Redshift
    -   Performed ETL to extract from staging tables, transform and create optimized tables for performing analytics
-   **Concepts Learned:**
    -   IAM Roles
    -   COPY from S3
    -   Distributed Columnar Database Design (DISTKEY, SORTKEY)
-   **Core Technologies Used:**
    -   Python (Pandas, Jupyter, psycopg2, boto3)
    -   Apache Cassandra
4. [Data Lake with Apache Spark](https://github.com/jason-huynh83/Udacity-Data-Engineering-Projects/tree/master/Data_Lake_Spark)

Set up a spark data lake using Amazon EMR that performs analytics on user activity data for sparkify - a music streaming app.

-   **Tasks Completed:**
    -   Administered a EMR Cluster on AWS(Created roles & users)
    -   Performed ETL to Read Data From S3 using PySpark, performs transformation and saves results as parquet files on S3
-   **Concepts Learned:**
    -   Schema On Read
    -   Data Lake Implementation Options on AWS
    -   Parquet Files
-   **Core Technologies Used:**
    -   Python (Pandas, PySpark)
    -   Apache Spark
    -   Amazon Elastic MapReduce(EMR)
  5. [Data Pipeline with Apache Airflow](https://github.com/jason-huynh83/Udacity-Data-Engineering-Projects/tree/master/Data_Pipelines_Airflow)
 
 Set up a data pipeline using Apache Airflow that schedules and monitors workflow for performing analytics on user activity data for sparkify - a music streaming app.

-   **Tasks Completed:**
    -   Created an Apache Airflow program to automate python scripts (Setup connections, Server, UI, Scheduler)
    -   Created an Amazon Redshift Database to load all the data
    -   Created custom operators for performing tasks to stage raw data to Redshift, load fact & dimension tables to redshift and perform quality checks on resulting data to ensure data is correct 
-   **Concepts Learned:**
    -   Directed Acyclic Graphs (DAGs) relevance to data pipelines and how to automate scripts
    -   Operators, Custom Operators, Tasks, Hooks, Connections, Context Templating 
    -   Data Lineage, Scheduling, Backfilling, Partitioning and Quality Checks on Apache Airflow
-   **Core Technologies Used:**
    -   Python 
    -   Apache Airflow