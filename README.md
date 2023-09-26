# Data ELT Pipeline Project

## What is this repo for?

This repository is a project consisting of an ELT pipeline using Airflow in a Docker container, S3 and BigQuery as storage layers:
1. Request the follwing endpoint to download information about weather foerecast in Mexico per day by municipality: https://smn.conagua.gob.mx/tools/GUI/webservices/?method=1 and https://smn.conagua.gob.mx/tools/GUI/webservices/?method=3 per hour.
2. Uploads the Data into an S3 Bucket
3. Load the raw data into BigQuery and computes the following aggregates:
4. Generate a sample query.
5. Generate a table that has the average temperature and precipitation by municipality of the last two hours. (WIP)
6. Generate a joined table between the first generated table and the latest pre-computed data (data_municipios). (WIP)

## How to use it?

### Dockerized version
**If you're working on Windows I encourage to you use WSL to clone the repository if you don't wanna have problems with the file directory structure because in windows you have to use `\`   insted of `/`**
1. Clone the repository and checkout the tag called: v1.2.0 Refactor Hourly Pipeline that has everything to start using it
2. Install Docker compose on your machine
3. Locate your terminal in the root folder of the repository
4. Add bq_sa.json file. This represents the Service Account for GCP. 
5. Add .env file using example.env as a template but adding your own AWS and GCP credentials. 
6. Execute  ```sudo docker-compose build``` and ```sudo docker-compose up -d```
7. Open in your web browser http://localhost:8080/home
8. Log in into the Airflow web UI user: airflow, password: airflow
9. Everything is setup


## About the Solution
I used Pandas as data manipulation layer because it offers a complete solution to the required tasks, Airflow as a pipeline orchestrator, AWS S3 for raw data storage (GZip Json) and Google BigQuery for structured data. Built a Custom Airflow Operator to move the data between the AWS S3 -> BigQuery    
### File structure
The file directory follows and standard etl pipeline structure, we have:
*   *airflow/* Includes:
    - common/ Directory containing all the common modules
        - custom_operators: Directory containing all the Airflow custom operators
        - aws: AWS custom classes for the use cae (wrapper of boto3)
        - gcp: Google Cloud custom classes for the use case (wrapper for gcp library)
        - utils.py: Helper common functions
    - dags/ directory where it's located the DAGS for bth pipelines
        - daily_pipeline.py: DAG
        - hourly_pipeline.py: DAG
    - *data/* Includes:
        - *data_municipios/*: Where it's stored static data about municipios (It is also stored in BigQuery but i placed here for the other users.)
    - Airflow config files.: *aiflow.cfg, airflow.db, webserver_config.py*
    - queries.toml (File containing queries to be run into BigQuery)
* *example.env* Env file of example (**You need to replace some env variables like S3 and BigQuery configuration)
* *requirements.txt* File
* *docker-compose.yml* File
* Dockerfile for building the custom docker image
* */example_data* Contains some sample executions
### Logic
I just did the following steps to complete the solution: 
1. Extract phase: Request the endpoint with the extract submodule that extracts the raw compressed file, stores it in an S3 bucket in raw format (Gzip Json).
2. Load phase: Move the Data between S3 to BigQuery using custom operators and push the raw table into BigQuery prior schema and data type validations.
4. Transform phase: Write aggregated tables into BigQuery from existing raw table, performing all the computing with BigQuery.

### Strengths of the solution
1. It uses state of the art tools like Docker and Airflow to execute and orchestrate the pipeline virtually everywhere
2. Modularized, atomic and well-documented code

### Further versions improvements and how to scale, organize and automate the solution:
1. Use cloud managed airflow 
2. Include a CI/CD/QA/DQ Flow
