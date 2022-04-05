#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
#from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from airflow.operators.python import PythonOperator
import requests
import json

#######################
##! 2. Default arguments
#######################

default_args = {
    'owner': 'jdpinedaj',
    'depends_on_past': False,
    'email': ['jpineda@option.cl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def get_data_upload_to_gcs(self):
    self.response = requests.post(url=URL_CF1)
    if self.response.status_code != 200:
        raise Exception(f"Error downloading data: {self.response.text}")
    else:
        return self.response


def raw_schema_processed(self):
    self.response = requests.post(url=URL_CF2)
    if self.response.status_code != 200:
        raise Exception(f"Error downloading data: {self.response.text}")
    else:
        return self.response


#######################
##! 3. Instantiate a DAG
#######################

#TODO: Esto es bueno ponerlo mejor en las Variables de Airflow?
URL_AIRLINES = "https://media.githubusercontent.com/media/dpinedaj/airflights-kaggle-data/master/airlines.csv"
URL_AIRPORTS = "https://media.githubusercontent.com/media/dpinedaj/airflights-kaggle-data/master/airports.csv"
URL_FLIGHTS = "https://media.githubusercontent.com/media/dpinedaj/airflights-kaggle-data/master/flights.csv"
PROJECT_ID = 'bi-fcom-drmb-loyalty-prd'
GCP_LOCATION = 'us-central1'
MY_DATASET = 'sandbox_ext_jjaramillo_temp'
GS_BUCKET = 'bucket_poc_airflow_ext_jjaramillo'
SCHEDULE_INTERVAL = '@once'
#CF1 = 'get-data-upload-to-gcs'
#CF2 = 'raw-schema-processed'
URL_CF1 = 'https://us-central1-bi-fcom-drmb-loyalty-prd.cloudfunctions.net/get-data-upload-to-gcs'
URL_CF2 = 'https://us-central1-bi-fcom-drmb-loyalty-prd.cloudfunctions.net/raw-schema-processed'

# Additional variables
date = datetime.now().strftime("%Y_%m_%d")

dag = DAG(dag_id='poc_juan_option_test',
          description='Juan probando en el PoC',
          start_date=datetime.now(),
          schedule_interval=SCHEDULE_INTERVAL,
          concurrency=5,
          max_active_runs=1,
          default_args=default_args)

#######################
##! 4. Tasks
#######################

#? 4.1. Starting pipeline

start_pipeline = DummyOperator(task_id='start_pipeline', dag=dag)

#? 4.2. Download data from kaggle in parquet, and upload it into gcs using CLOUD FUNCTIONS

download_airlines_data = PythonOperator(
    task_id='download_airlines_data',
    python_callable=get_data_upload_to_gcs,
    op_kwargs=json.dumps({
        "bucket_name": GS_BUCKET,
        "source_url": URL_AIRLINES,
        "destiny_path": f"raw/{date}_airlines.parquet"
    }),
    dag=dag,
)
download_airports_data = PythonOperator(
    task_id='download_airports_data',
    python_callable=get_data_upload_to_gcs,
    op_kwargs=json.dumps({
        "bucket_name": GS_BUCKET,
        "source_url": URL_AIRPORTS,
        "destiny_path": f"raw/{date}_airports.parquet"
    }),
    dag=dag,
)

download_flights_data = PythonOperator(
    task_id='download_flights_data',
    python_callable=get_data_upload_to_gcs,
    op_kwargs=json.dumps({
        "bucket_name": GS_BUCKET,
        "source_url": URL_FLIGHTS,
        "destiny_path": f"raw/{date}_flights.parquet"
    }),
    dag=dag,
)

# download_flights_data = CloudFunctionInvokeFunctionOperator(
#     task_id="download_flights_data",
#     project_id=PROJECT_ID,
#     location=GCP_LOCATION,
#     input_data=json.dumps({
#         "bucket_name": GS_BUCKET,
#         "source_url": URL_FLIGHTS,
#         "destiny_path": f"raw/{date}_flights.parquet"
#     }),
#     function_id=CF1,
#     dag=dag)

#? 4.3. Change schema to raw_data and load it again in processed_data

processing_airlines_data = PythonOperator(
    task_id='processing_airlines_data',
    python_callable=raw_schema_processed,
    op_kwargs=json.dumps({
        "bucket_name": GS_BUCKET,
        "origin_path": f"raw/{date}_airlines.parquet",
        "destiny_path": f"processed/{date}_airlines.parquet",
        "schema_name": "airlines_schema.json",
    }),
    dag=dag,
)

processing_airports_data = PythonOperator(
    task_id='processing_airports_data',
    python_callable=raw_schema_processed,
    op_kwargs=json.dumps({
        "bucket_name": GS_BUCKET,
        "origin_path": f"raw/{date}_airports.parquet",
        "destiny_path": f"processed/{date}_airports.parquet",
        "schema_name": "airports_schema.json",
    }),
    dag=dag,
)

processing_flights_data = PythonOperator(
    task_id='processing_flights_data',
    python_callable=raw_schema_processed,
    op_kwargs=json.dumps({
        "bucket_name": GS_BUCKET,
        "origin_path": f"raw/{date}_flights.parquet",
        "destiny_path": f"processed/{date}_flights.parquet",
        "schema_name": "flights_schema.json",
    }),
    dag=dag,
)

# processing_flights_data = CloudFunctionInvokeFunctionOperator(
#     task_id="processing_flights_data",
#     project_id=PROJECT_ID,
#     location=GCP_LOCATION,
#     input_data=json.dumps({
#         "bucket_name": GS_BUCKET,
#         "origin_path": f"raw/{date}_flights.parquet",
#         "destiny_path": f"processed/{date}_flights.parquet",
#         "schema_name": "flights_schema.json",
#     }),
#     function_id=CF2,
#     dag=dag)

#? 4.4. Load data from gcs to bigquery

load_airlines_data = GCSToBigQueryOperator(
    task_id='load_airlines_data',
    bucket=GS_BUCKET,
    source_objects=[f"processed/{date}_airlines.parquet"],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{MY_DATASET}.airlines_data',
    source_format='parquet',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
    dag=dag)

load_airports_data = GCSToBigQueryOperator(
    task_id='load_airports_data',
    bucket=GS_BUCKET,
    source_objects=[f"processed/{date}_airports.parquet"],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{MY_DATASET}.airports_data',
    source_format='parquet',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
    dag=dag)

load_flights_data = GCSToBigQueryOperator(
    task_id='load_flights_data',
    bucket=GS_BUCKET,
    source_objects=[f"processed/{date}_flights.parquet"],
    destination_project_dataset_table=f'{PROJECT_ID}:{MY_DATASET}.flights_data',
    source_format='parquet',
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
    dag=dag)

#? 4.5. Data check

check_airlines = BigQueryCheckOperator(
    task_id='check_airlines',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{MY_DATASET}.airlines_data`',
    dag=dag)

check_airports = BigQueryCheckOperator(
    task_id='check_airports',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{MY_DATASET}.airports_data`',
    dag=dag)

check_flights = BigQueryCheckOperator(
    task_id='check_flights',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{MY_DATASET}.flights_data`',
    dag=dag)

loaded_data_to_bigquery = DummyOperator(task_id='loaded_data', dag=dag)

#? 4.6. Generating a view

check_unified_view = BigQueryCheckOperator(
    task_id='check_unified_view',
    use_legacy_sql=False,
    bigquery_conn_id=PROJECT_ID,
    params={
        'project_id': PROJECT_ID,
        'my_dataset': MY_DATASET
    },
    #sql='./sql/unified_view.sql', #TODO: Este archivo hay que meterlo al bucket
    sql='''
        WITH flights_airlines AS (
            SELECT
                flights.year,
                flights.month,
                flights.day,
                flights.airline as airline_iata_code,
                airlines.airline,
                flights.flight_number,
                flights.origin_airport
            FROM `{{ params.project_id }}.{{ params.my_dataset }}.flights` flights
            LEFT JOIN `{{ params.project_id }}.{{ params.my_dataset }}.airlines` airlines
            ON flights.airline = airlines.iata_code
            )
            SELECT 
                year,
                month,
                day,
                airline_iata_code,
                airline,
                flight_number,
                origin_airport,
                airports.airport AS name_airport,
                airports.city,
                airports.state,
                airports.latitude,
                airports.longitude
            FROM flights_airlines
            LEFT JOIN `{{ params.project_id }}.{{ params.my_dataset }}.airports` airports
            ON flights_airlines.origin_airport = airports.iata
            LIMIT 30
        ''',
    dag=dag)

#? 4.7. Finishing pipeline

finish_pipeline = DummyOperator(task_id='finish_pipeline', dag=dag)

#######################
##! 5. Setting up dependencies
#######################

start_pipeline >> [
    download_airlines_data, download_airports_data, download_flights_data
]

download_airlines_data >> processing_airlines_data >> load_airlines_data >> check_airlines
download_airports_data >> processing_airports_data >> load_airports_data >> check_airports
download_flights_data >> processing_flights_data >> load_flights_data >> check_flights

[check_airlines, check_airports, check_flights
 ] >> loaded_data_to_bigquery >> check_unified_view >> finish_pipeline
