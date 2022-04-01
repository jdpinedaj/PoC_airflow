#######################
## 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCheckOperator, BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

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
STAGING_DATASET = 'sandbox_ext_jjaramillo_temp'
DWH_DATASET = 'sandbox_ext_jjaramillo'
GS_BUCKET = 'bucket_poc_airflow_ext_jjaramillo'
SCHEDULE_INTERVAL = '@once'


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


##### Creating empty table in BigQuery #TODO1 Esto es necesario?


# SCHEMA = [
#     {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
#     {"name": "name", "type": "STRING", "mode": "NULLABLE"},
#     {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
# ] #TODO2: Este esquema se puede poner para las 3 tablas en un archivo aparte?
    #TODO2:  Ademas, es necesario especificar primero para la raw data todo en STRING y luego en la processed data las verdaderas?

# create_dataset = BigQueryCreateEmptyDatasetOperator(
#                 task_id="create-dataset",
#                 dataset_id=DATASET_NAME,
#                 location=location,
#                 )

# create_table_1 = BigQueryCreateEmptyTableOperator(
#     task_id="create_table_1",
#     dataset_id=DATASET_NAME,
#     table_id=TABLE_1,
#             schema_fields=SCHEMA,
#              location=location,
#         )



##### Download data from kaggle in parquet, and upload it into gcs using CLOUD FUNCTIONS


#TODO USAR CLOUD FUNCTIONS CON REQUEST, GENERICA

download_airlines_data = ()

download_airports_data = ()

download_flights_data = ()



##### Load data from gcs to bigquery
#TODO: Could I bring the schemas from an external file such as in 
#TODO: src_pruebas_que_pueden_ser_utiles/staging_schemas.py?

load_airlines_data = GCSToBigQueryOperator(
    task_id='load_airlines_data',
    bucket=GS_BUCKET,
    source_objects=['airlines.parquet'],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{STAGING_DATASET}.airlines_data',
    source_format='parquet',
    schema_fields=[
            {'name': 'iata_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'airline', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    dag=dag)

load_airports_data = GCSToBigQueryOperator(
    task_id='load_airports_data',
    bucket=GS_BUCKET,
    source_objects=['airports.parquet'],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{STAGING_DATASET}.airports_data',
    source_format='parquet',
    schema_fields=[
            {'name': 'iata', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'airport', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    dag=dag)

load_flights_data = GCSToBigQueryOperator(
    task_id='load_flights_data',
    bucket=GS_BUCKET,
    source_objects=['flights.parquet'],
    destination_project_dataset_table=
    f'{PROJECT_ID}:{STAGING_DATASET}.flights_data',
    source_format='parquet',
    schema_fields=[
            {'name': 'year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'month', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'day', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'day_of_week', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'airline', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'flight_number', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'tail_number', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'origin_airport', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'destination_airport', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'scheduled_departure', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'departure_time', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'departure_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'taxi_out', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'wheels_off', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'scheduled_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'elapsed_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'air_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'distance', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'wheels_on', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'taxi_in', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'scheduled_arrival', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'arrival_time', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'arrival_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'diverted', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'cancelled', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'cancellation_reason', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'air_system_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'security_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'airline_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'late_aircraft_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'weather_delay', 'type': 'INTEGER', 'mode': 'NULLABLE'},           
        ],
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    autodetect=False,
    dag=dag)

##### Data check

check_airlines = BigQueryCheckOperator(
    task_id='check_airlines',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.airlines_data`',
    dag=dag)

check_airports = BigQueryCheckOperator(
    task_id='check_airports',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.airports_data`',
    dag=dag)

check_flights = BigQueryCheckOperator(
    task_id='check_flights',
    use_legacy_sql=False,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.flights_data`',
    dag=dag)

loaded_data_to_staging = DummyOperator(task_id='loaded_data',
dag=dag)

##### Transform, load, and check fact data
#TODO6: Aqui es donde se hace el casteo? En la carpeta sql debo de definir los tipos de las variables?

create_airlines_data = BigQueryExecuteQueryOperator(
    task_id='create_airlines_data',
    use_legacy_sql=False,
    destination_dataset_table=
    '{PROJECT_ID}.{DWH_DATASET}.airlines${{ds_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=PROJECT_ID,
    params={
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql='./sql/airlines.sql',
    dag=dag)

create_airports_data = BigQueryExecuteQueryOperator(
    task_id='create_airports_data',
    use_legacy_sql=False,
    destination_dataset_table=
    '{PROJECT_ID}.{DWH_DATASET}.airports${{ds_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=PROJECT_ID,
    params={
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql='./sql/airports.sql',
    dag=dag)

create_flights_data = BigQueryExecuteQueryOperator(
    task_id='create_flights_data',
    use_legacy_sql=False,
    destination_dataset_table=
    '{PROJECT_ID}.{DWH_DATASET}.flights${{ds_nodash}}',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=PROJECT_ID,
    params={
        'project_id': PROJECT_ID,
        'staging_dataset': STAGING_DATASET,
        'dwh_dataset': DWH_DATASET
    },
    sql='./sql/flights.sql',
    dag=dag)

##### Generating a view

check_unified_view = BigQueryCheckOperator(
    task_id='check_unified_view',
    use_legacy_sql=False,
    bigquery_conn_id=PROJECT_ID,
    params={
        'project_id': PROJECT_ID,
        'dwh_dataset': DWH_DATASET},
        sql='./sql/unified_view.sql'
        dag=dag)

##### Finishing pipeline

finish_pipeline = DummyOperator(task_id='finish_pipeline',
dag=dag)

#######################
## 5. Setting up dependencies
#######################

dag >> start_pipeline >> [
    download_airlines_data, download_airports_data, download_flights_data
] >> [load_airlines_data, load_airports_data, load_flights_data]

load_airlines_data >> check_airlines
load_airports_data >> check_airports
load_flights_data >> check_flights

[check_airlines, check_airports, check_flights] >> loaded_data_to_staging

loaded_data_to_staging >> [
    create_airlines_data, create_airports_data, create_flights_data
] >> check_unified_view >> finish_pipeline
