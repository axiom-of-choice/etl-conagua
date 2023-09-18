# Data Engineering Challenge

## What is this repo for?

This repository is a project consisting of an ETL process using Airflow in a Docker container using S3 and BigQuery as storage layers:
1. Request the follwing endpoint to download information about weather foerecast in Mexico per hour by municipality: https://smn.conagua.gob.mx/tools/GUI/webservices/?method=1 and https://smn.conagua.gob.mx/tools/GUI/webservices/?method=3 each hour
2. Uploads the Data into S3 Bucket
3. Generate a table that has the average temperature and precipitation by municipality of the last two hours, versioned by datetime of execution truncated to minutes
4. Generate a joined table between the first generated table and the latest pre-computed data (data_municipios) versioned by datetime of execution truncated to minutes
5. Load the data into BigQuery and computes aggregate

## How to use it?

### Dockerized version
**If you're working on Windows I encourage to you use WSL to clone the repository if you don't wanna have problems with the file directory structure because in windows you have to use `\`   insted of `/`**
1. Clone the repository and checkout the tag called: v1.1.0-docker that has everything to start using it
2. Install Docker compose on your machine
3. Locate your terminal in the root folder of the repository
4. Execute  ```sudo docker-compose build``` and ```sudo docker-compose up -d```
5. Open in your web browser http://localhost:8080/home
6. Log in into the Airflow web UI user: airflow, password: airflow
7. Everything is setup


## About the Solution
I used Pandas as data manipulation layer because it offers a complete solution to the required tasks, Airflow as a pipeline orchestrator and standard python libraries to request an uncompress the data
### File structure
The file directory follows and standard etl pipeline structure, we have:
*   *airflow/* Includes:
    - dags/ directory where it's located the etls modules and custom operators that we will be using into the pipelines DAG to mantain the DAG file clean and organized
    - *data/* Includes:
        - *data_municipios/*: Where it's stored static data about municipios (It should be in another place like a Database or storage service)
    - Airflow config files.: *aiflow.cfg, airflow.db, webserver_config.py*
    - queries.toml (File containing queries to be run into BigQuery)
* *example.env* Env file of example 
* *requirements.txt* File
* *docker-compose.yml* File
* Dockerfile for building the custom docker image
* */example_data* Contains some sample executions
### Logic
I just did the following steps to complete the solution: 
1. Extract phase: Request the endpoint with the extract submodule that extracts the raw compressed file, stores it in an S3 bucket.
2. Load phase: Generate the table 1 using custom operators and push the raw table into BigQuery prior schema and data type validations.
3. Transform phase: Write aggregated tables into BigQuery from existing raw table.

### Strengths of the solution
1. It uses state of the art tools like Docker and Airflow to execute and orchestrate the pipeline virtually everywhere
2. Modularized, atomic and well-documented code

### Further versions improvements and how to scale, organize and automate the solution:
1. Use cloud managed airflow 
2. Include a CI/CD/QA/DQ Flow