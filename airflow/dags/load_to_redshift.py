import datetime
import logging
import sys

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.operators.postgres_operator import PostgresOperator

# Import sql_statements.py
import sql_statements

@dag(
    start_date=datetime.datetime.now(),
    tags=['redshift']
)
def redshift_dag():
    
    create_tables = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
    
    @task()
    def load_data_to_redshift():
        aws_hook = AwsGenericHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        postgres_operator = PostgresOperator(
            task_id="load_data_to_redshift",
            postgres_conn_id="redshift",
            sql=sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key)
        )
            
    load_data_to_redshift = load_data_to_redshift()
            
    location_traffic_task = PostgresOperator(
        task_id="location_traffic_task",
        postgres_conn_id="redshift",
        sql=sql_statements.LOCATION_TRAFFIC_SQL
    )
    
    create_tables >> load_data_to_redshift >> location_traffic_task
    
redshift_dag()


            
    
