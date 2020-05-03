# odd-airflow
Airflow instance supporting [on-da-dip](https://www.github.com/jzj59/on-da-dip) stock and securities data. 
Contains pipelines fetching data from public securities APIs, reshaping and landing data into s3, and loading data into databases. 

**note**: please read [docker_README.md](https://www.github.com/jzj59/odd-airflow/docker_README.md) to stand up Airflow instance.

### Project Structure
```text
odd-airflow
|-- config ... Airflow config file
|   |-- airflow.cfg
|
|-- creds ... txt files for storing credentials, these are git ignored. Need a less manual way of deploying these
|   |-- tokeinfo.txt
|   |-- aws_creds.txt
|   |-- api_secrets.txt
|
|-- dags ... Airflow dags folder
|   |-- utilities ... useful classes and methods for pipelines (managing s3, api auth, etc.)
|   |   |-- tda_client.py
|   |   |-- s3_management.py
|   |-- dag1
|   |   |-- dag1_definition.py
|   |   |-- helpers/
|   |   |   |-- dag1_helper.py
|   |-- dag2
|   |   |-- dag2_definition.py
|
|-- plugins ... Airflow plugins (operators/hooks/etc.)
|
|-- requirements ... requirements txt files (Python, R, etc.)
|   |-- requirements.txt
|-- script ... Docker initialization scripts
|   |-- entrypoint.sh
|   
|-- docker-compose-CeleryExecutor.yml: yaml compose config for a Celery based Airflow instance
|-- Dockerfile: Airflow Docker image, include DAG python dependencies
  
```
  
**Example DAGs:**

get and store option chain:
- auth with TDA api and fetch current option chain for a given ticker symbol
- unpack JSON blob and convert to tabular dataframe
- upload dataframe to s3 bucket
 