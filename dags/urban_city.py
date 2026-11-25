from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

from include.transform import transform_data as transformed_data
from include.upload_data import upload_data

default_args = {
    'owner':'levis',
    'depends_on_past': False,
    'start_date' : datetime(2026, 6,22),
    'retries':1,
    'retry_delay':timedelta(minutes=1),
    'schedule_interval' : '@hourly'
}

@task()
def extract_data_from_api():
    api_response = upload_data()

    return api_response

@task()
def transform_data():
    cleaned_data= transformed_data()

    return cleaned_data

with DAG(dag_id='urban_city_requests',
         catchup=False, default_args=default_args):

    create_db_table = SQLExecuteQueryOperator(
        task_id='urban_city_table',
        sql='sql/urban_city.sql',
        conn_id='postgres_conn'
    )

    data_factory = AzureDataFactoryRunPipelineOperator(
        task_id='run_data_factory',
        pipeline_name='urban_city_factory'
    )

    create_db_table >> data_factory


    