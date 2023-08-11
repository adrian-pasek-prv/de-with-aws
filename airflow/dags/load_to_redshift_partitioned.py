import datetime
import logging
import sys

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.operators.postgres_operator import PostgresOperator

# Import sql_statements.py
import sql_statements

@dag(
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2019, 1, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    # Run one dag at a time
    max_active_runs=1
)
def redshift_dag_partitioned():
    
    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
    
    @task()
    def copy_trips_from_s3_to_redshift(*args, **kwargs):
        aws_hook = AwsGenericHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        ## **kwargs will be populated with Airflow context variables like dag_run.logical_date etc.
        execution_date = kwargs["dag_run.logical_date"]
        postgres_operator = PostgresOperator(
            task_id="copy_trips_from_s3_to_redshift",
            postgres_conn_id="redshift",
            # This is where partitioning happens. We use the date as the partition.
            sql=sql_statements.COPY_MONTHLY_TRIPS_SQL.format(credentials.access_key,
                                                         credentials.secret_key,
                                                         year=execution_date.year,
                                                         month=execution_date.month)
        )
            
    copy_trips_from_s3_to_redshift = copy_trips_from_s3_to_redshift()
    
    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    )
      
    @task()
    def copy_stations_from_s3_to_redshift():
        aws_hook = AwsGenericHook("aws_credentials")
        credentials = aws_hook.get_credentials()    
        postgres_operartor  = PostgresOperator(
            task_id="copy_stations_from_s3_to_redshift",
            postgres_conn_id="redshift",
            sql=sql_statements.COPY_STATIONS_SQL.format(
                credentials.access_key,
                credentials.secret_key,
            )
    )
        
    copy_stations_from_s3_to_redshift = copy_stations_from_s3_to_redshift()
    
    # In this form, we can run conceptually isolated tasks in parallel.
    create_trips_table >> copy_trips_from_s3_to_redshift
    create_stations_table >> copy_stations_from_s3_to_redshift
    
redshift_dag_partitioned()


            
    
