# La Liga ETL

ETL process that scrapes data from Understat (website that gathers information about football matches) API locally, transforms it also locally and loads into BigQuery.
There are two datasets that have matches already played and a table specifying team statistics based on first dataset. 
Script runs weekly (Airflow triggers ETL process) because the matches usually take place this way. There's a sample dashboard created in Data Studio which contains laliga data.

(I know that table doesn't seem like the google table but it's still WIP)

![App Screenshot](https://i.ibb.co/r57ZCZk/DD.png)

and 

![App Screenshot](https://i.ibb.co/q59Gytj/DDD.png)
## Acknowledgements

 - [BigQuery locations](https://cloud.google.com/bigquery/docs/locations)
 - [Create storage buckets](https://cloud.google.com/storage/docs/creating-buckets)
 - [Obtain credentials to storage account](https://developers.google.com/workspace/guides/create-credentials)


# Prerequisites

[Docker](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/) v1.27.0

[Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)


To run this project, you will need to add the following environment variables to your docker-compose file and create GCS bucket


`PROJECT_ID`

`BUCKET`

`BIGQUERY_DATASET` 

`LOCATION`
 


Google Cloud will require login credentials for authorization so you will need to paste the file "google_credentials.json" to ~/.google/credentials


## Run Locally

Clone the project

```bash
  git clone https://github.com/mwacinski/laligatable
```

Go to the project directory

```bash
  cd laligatable
```
Run Docker 

```bash
 docker-compose up --build -d
 ```
