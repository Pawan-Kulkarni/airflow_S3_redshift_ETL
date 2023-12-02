# ETL with Airflow , S3 and Redshift.

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The goal is  to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


## Project Workflow

The data for this project resides on the following S3 Public Buckets:

For this project, we'll be working with two datasets. Here are the s3 links for each:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

This data should first be brought into our AWS S3 environment.
After that , log and song data should be stored in staging tables.
We then will carry out ETL process and fill the data in facts and dimensional tables. 
Lastly , we will carry out some data quality checks .
The workflow for this entire dag process in shown below:

![Dag workflow](https://github.com/Pawan-Kulkarni/airflow_S3_redshift_ETL/blob/main/example-dag.png?raw=true)
