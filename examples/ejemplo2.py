from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators import bigquery_operator


# GCP configuration
#PROJECT_ID=models.Variable.get("GCP_PROJECT")
GCP_CONNECTION_ID = 'google_cloud_default'
QUERY=models.Variable.get("ejemplo1")

# DAG configuration
DAG_NAME = 'dag_ejemplo6'
PROCESS_DATE='{{ ds }}'
PROCESS_DATE_NODASH='{{ ds_nodash }}'

default_args = {
    'owner': 'Fcom',
    'depends_on_past': False,
    'start_date': datetime(2000, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2) #,
    #'on_failure_callback': task_fail_slack_alert
}

with models.DAG(
    DAG_NAME,
    catchup=False,
    default_args=default_args,
    params={

    },
    user_defined_macros={"fecha": PROCESS_DATE},
    schedule_interval='1 8 * * *') as dag: #'*/45 * * * *'


    get_data = bigquery_operator.BigQueryOperator(
        task_id='get_data',
        sql=QUERY,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table="tc-sc-bi-bigdata-fcom-dtch-dev:sandbox_agcarcamo.ejemplo1${partition_date}".format(partition_date=PROCESS_DATE_NODASH),
        time_partitioning={'type': 'DAY', 'requirePartitionFilter': True}
        #,
        #query_params=[{'name': 'fecha', 'parameterType': { 'type': 'STRING' }, 'parameterValue': { 'value': '2022-03-22'} }]
    )
    
    get_data



