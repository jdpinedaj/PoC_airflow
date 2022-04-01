#######################
## 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCheckOperator, BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.http.operators import http
import json

#######################
## 2. Default arguments
#######################

default_args = {
    'owner': 'jdpinedaj',
    'depends_on_past': False,
    'email': ['jpineda@option.cl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

#######################
## 3. Instantiate a DAG
#######################

#TODO: Esto es bueno ponerlo mejor en las Variables de Airflow?
PROJECT_ID = 'bi-fcom-drmb-loyalty-prd'
MY_DATASET = 'sandbox_ext_jjaramillo_temp'
GS_BUCKET = 'bucket_poc_airflow_ext_jjaramillo'
SCHEDULE_INTERVAL = '@once'
CF1 = 'https://us-central1-bi-fcom-drmb-loyalty-prd.cloudfunctions.net/get-data-upload-to-gcs'

# Additional variables
date = datetime.now().strftime("%Y_%m_%d")


dag = DAG(
    dag_id='poc_juan_option_test',
    default_args=default_args,
    description='Juan probando en el PoC',
    start_date=datetime(2022, 2, 28),
    schedule_interval=SCHEDULE_INTERVAL,
    concurrency=5,
    max_active_runs=1,
)

#######################
## 4. Tasks
#######################

##### Starting pipeline

start_pipeline = DummyOperator(task_id='start_pipeline', dag=dag)

##### Download data from kaggle in parquet, and upload it into gcs using CLOUD FUNCTIONS

download_airlines_data = http(
    task_id= 'download_airlines_data',
    method='POST',
    http_conn_id=CF1,
    endpoint='main',
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "bucket_name": "bucket_poc_airflow_ext_jjaramillo",
        "source_url": "https://media.githubusercontent.com/media/dpinedaj/airflights-kaggle-data/master/airlines.csv", 
        "destiny_path": f"raw/{date}_airlines.parquet"
        }),  # possible request parameters
    dag=dag
)

download_airports_data = http(
    task_id= 'download_airports_data',
    method='POST',
    http_conn_id=CF1,
    endpoint='main',
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "bucket_name": "bucket_poc_airflow_ext_jjaramillo",
        "source_url": "https://media.githubusercontent.com/media/dpinedaj/airflights-kaggle-data/master/airports.csv", 
        "destiny_path": f"raw/{date}_airports.parquet"
        }),  # possible request parameters
    dag=dag
)

download_flights_data = http(
    task_id= 'download_flights_data',
    method='POST',
    http_conn_id=CF1,
    endpoint='main',
    headers={"Content-Type": "application/json"},
    data=json.dumps({
        "bucket_name": "bucket_poc_airflow_ext_jjaramillo",
        "source_url": "https://media.githubusercontent.com/media/dpinedaj/airflights-kaggle-data/master/flights.csv", 
        "destiny_path": f"raw/{date}_flights.parquet"
        }),  # possible request parameters
    dag=dag
)


##### Change schema to raw_data and load it again in processed_data
#TODO: Terminar el CF2

processing_airlines_data = operator()

processing_airports_data = operator()

processing_flights_data = operator()



##### Load data from gcs to bigquery
#TODO: Could I bring the schemas from an external file such as in 
#TODO: src_pruebas_que_pueden_ser_utiles/staging_schemas.py?

load_airlines_data = GCSToBigQueryOperator(
    task_id='load_airlines_data',
    bucket=GS_BUCKET,
    source_objects=['airlines.parquet'],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{MY_DATASET}.airlines_data',
    source_format='parquet',
    # schema_fields=[
    #         {'name': 'iata_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'airline', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
    dag=dag)

load_airports_data = GCSToBigQueryOperator(
    task_id='load_airports_data',
    bucket=GS_BUCKET,
    source_objects=['airports.parquet'],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{MY_DATASET}.airports_data',
    source_format='parquet',
    # schema_fields=[
    #         {'name': 'iata', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'airport', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #     ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
    dag=dag)

load_flights_data = GCSToBigQueryOperator(
    task_id='load_flights_data',
    bucket=GS_BUCKET,
    source_objects=['flights.parquet'],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{MY_DATASET}.flights_data',
    source_format='parquet',
    # schema_fields=[
    #         {'name': 'year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'day_of_week', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'airline', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'flight_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'tail_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'origin_airport', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'destination_airport', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'scheduled_departure', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'departure_time', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'departure_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'taxi_out', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'wheels_off', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'scheduled_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'elapsed_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'air_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'distance', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'wheels_on', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'taxi_in', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'scheduled_arrival', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'arrival_time', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'arrival_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'diverted', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'cancelled', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    #         {'name': 'cancellation_reason', 'type': 'STRING', 'mode': 'NULLABLE'},
    #         {'name': 'air_system_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'security_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'airline_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'late_aircraft_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #         {'name': 'weather_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},           
    #     ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=True,
    dag=dag)

##### Data check

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

loaded_data_to_bigquery = DummyOperator(task_id='loaded_data',
dag=dag)


##### Generating a view

check_unified_view = BigQueryCheckOperator(
    task_id='check_unified_view',
    use_legacy_sql=False,
    bigquery_conn_id=PROJECT_ID,
    params={
        'project_id': PROJECT_ID,
        'my_dataset': MY_DATASET},
        sql='./sql/unified_view.sql' #TODO: Este archivo hay que meterlo al bucket
        dag=dag)

##### Finishing pipeline

finish_pipeline = DummyOperator(task_id='finish_pipeline',
dag=dag)

#######################
## 5. Setting up dependencies
#######################

dag >> start_pipeline >> 
[download_airlines_data, download_airports_data, download_flights_data] >> 
[processing_airlines_data, processing_airports_data, processing_flights_data] >>
[load_airlines_data, load_airports_data, load_flights_data]

download_airlines_data >> processing_airlines_data >> load_airlines_data >> check_airlines
download_airports_data >> processing_airports_data >> load_airports_data >> check_airports
download_flights_data >> processing_flights_data >> load_flights_data >> check_flights

[check_airlines, check_airports, check_flights] >> check_unified_view

check_unified_view >> finish_pipeline
