# Data Engineering Challenge

## What is this repo for?

This repository contains a solution to the challenge for the Data Engineer Position.

## How to use it?
There are two ways to use the repo:


### Dockerized version
1. Download the tag called: v1.0.0-docker that has everything to start using it
2. Install Docker compose on your machine
3. Locate your terminal in the root folder of the repository
4. Execute  ```sudo docker-compose up -d```
5. Open in your web browser http://localhost:8080/home
6. Log in into the Airflow web UI user: airflow, password: airflow
7. Everything is setup 


### Local version
1. Download the tag called: v1.0.0-local that has everything to start using it
2. Locate yout terminals in the root folder of this repository
3. Setup and enter a virtual environment 
    ```python -m venv venv
     source venv/bin/activate
     ```
4. Set AIRFLOW_HOME
    ```export AIRFLOW_HOME=`pwd`/airflow```
5. Install requirements.txt ```pip install -r requirements.txt```
4. Execute  ```sudo docker-compose up -d```
5. Initialise a Database with an user
```
airflow db init

airflow users create \
    --username airflow \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --password airflow \
    --email spiderman@superhero.org

```
6. Start the services
Use two different terminals, remember to activate your Python environment and set the environment variables!
    6.1 Start Web server
    ```airflow webserver --port 8080```
    6.2 Start Scheduler
    ```airflow scheduler```
5. Open in your web browser http://localhost:8080/home
6. Log in into the Airflow web UI user: airflow, password: airflow
8. Manipulate your airflow.cfg setting ```enable_xcom_pickling = True```
7. Everything is setup 

## About the Solution
I used Pandas as data manipulation layer because it offers a complete solution to the required tasks, Airflow as a pipeline orchestrator and standard python libraries to request an uncompress the data
### File structure
The file directory follows and standard etl pipeline structure, we have:
*   *airflow/* Includes:
    - dags/ directory where it's located the weather_etl module that has extract, load, transform and utils submodules that we will be using into the pipeline_weather DAG to mantain the DAG file clean and organized
        - *data/* Includes:
            - *data_municipios/*: Where it's stored data about municipios (It should be in another place like a Database or storage service)
            - *intermediate/*: We use this as an intermediate pre-processed storage layer that stores the data requested to the web-service uncompressed into json format (But in production we should use some S3 or Google Cloud Storage)
            - *processed/*
                - */process_1* It stores the data required for second point of the exercise (We should store it into a external Database or Storage)
                - */process_1* It stores the data required for third and fourth point of the exercise (We should store it into a external Database or Storage)
            - *raw/*: Stores the raw data requested to the web-service in a compressed gzip format (But in production we should use some S3 or Google Cloud Storage)
        - */logs*: Stores the logs of the DAG execution generated by Airflow
        - */plugins*: Airflow plugins directory
        - Airflow config files.: *aiflow.cfg, airflow.db, webserver_config.py*
* */scripts* Bash scripts
* */Tests* Unit testing
* *.env* file
* *requirements.txt* File
* *docker-compose.yml* File
### Logic
I just did the following steps to complete the solution: 
1. Extract phase: Request the endpoint with the extract submodule that extracts the raw compressed file, stores it in the raw layer, uncompress the file and transform it in json format, store it in intermediate layer.
2. Transform phase: Generate the table 1 using the transform submodule and **push the table using the XCOM Airflow backend (we should store it in some place first, but for the purpouses of this exercise this works)** and then generate the table that queries this file using the XCOM backend, generate the second table that depends directly into first table and then:
3. Load phase: Write the generated tables into the local storage labelling properly the datasets (We should store it somewhere outside the container or local storage)
5. Cleaning phase: Just clean the staging folders containing raw and intermediate data also the XCOM storage.

### Strengths of the solution
1. It uses state of the art tools like Docker and Airflow to execute and orchestrate the pipeline virtually everywhere
2. Modularized, atomic and well-documented code

### Weaknesses
1. It doesn't use external storage and it should.
2. Some functions are not optimal having some file routes harcoded for practical purpouses
3. I didn't include tests but in a production environment it should.

### Further versions improvements and how to scale, organize and automate the solution:
1. Use cloud managed airflow 
2. Include a CI/CD/QA/DQ Flow
3. Use external cloud storage everywhere is possible 