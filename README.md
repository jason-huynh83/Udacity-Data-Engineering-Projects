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