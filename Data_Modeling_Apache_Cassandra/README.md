# Sparkify's Data Modeling With Apache Cassandra
## Introduction
- Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
- The project sets up an ETL workflow that creates the data models in apache cassandra database.
## Datasets
- This project only uses one dataset, event_data. The directory of CSV files are partitioned by date. 
## Modeling your NoSQL Database (Apache Cassandra Database)
1. Design tables to answer queries
2. Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
3. Develop your CREATE statement for each of the tables to address each question
4. Load the data with INSERT statement for each of the tables
5. Include IF NOT EXISTS clauses in CREATE statements to create tables only if the tables do not already exist. After, use DROP TABLE statements for each table. 
6. Test by running the proper select statements with the correct WHERE clause

## Build ETL Pipeline
1. Implement the logic in section Part I of the notebook to iterate through each event file in event_data to process and create a new CSV file in python
2. Make necessary edits to include Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in your data model
3. Test by running three select statements after running the queries on database
4. Drop tables and shutdown cluster