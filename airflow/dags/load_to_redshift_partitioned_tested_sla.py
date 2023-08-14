import datetime
import logging
import sys

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



# Import sql_statements.py
import sql_statements

# Data quality check if copy operation returned records > 0
def check_greater_than_zero(*args, **kwargs):
    # Table name that will be passed in "params" dict when this function is called
    table = kwargs["params"]["table"]
    postgres_hook = PostgresHook(postgres_conn_id="redshift")
    records = postgres_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    
    if len(records) < 1 or len(records[0]) < 1:
        logging.error(f"Data quality check failed. '{table}' returned no results")
        raise ValueError(f"Data quality check failed. '{table}' returned no results")
    
    num_of_records = records[0][0]
    logging.info(f"Data quality on table {table} check passed with {num_of_records:,} records")
    
    

@dag(
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2019, 1, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    # Run one dag at a time
    max_active_runs=1
)
def redshift_dag_partitioned_tested_sla():
    
    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
    
    @task(sla=datetime.timedelta(hours=1))
    def copy_trips_from_s3_to_redshift(*args, **kwargs):
        aws_hook = AwsGenericHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        ## **kwargs will be populated with Airflow context variables like data_interval_start etc.
        execution_date = kwargs["data_interval_start"]
        PostgresOperator(
            task_id="copy_trips_from_s3_to_redshift",
            postgres_conn_id="redshift",
            # This is where partitioning happens. We use the date as the partition.
            sql=sql_statements.COPY_MONTHLY_TRIPS_SQL.format(credentials.access_key,
                                                         credentials.secret_key,
                                                         year=execution_date.year,
                                                         month=execution_date.month)
        )
        check_greater_than_zero(params={"table": "trips"})
            
    copy_trips_from_s3_to_redshift = copy_trips_from_s3_to_redshift()
    
    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    )
      
    @task(sla=datetime.timedelta(hours=1))
    def copy_stations_from_s3_to_redshift():
        aws_hook = AwsGenericHook("aws_credentials")
        credentials = aws_hook.get_credentials()    
        PostgresOperator(
            task_id="copy_stations_from_s3_to_redshift",
            postgres_conn_id="redshift",
            sql=sql_statements.COPY_STATIONS_SQL.format(
                credentials.access_key,
                credentials.secret_key,
            )
        )
        check_greater_than_zero(params={"table": "stations"})
        
    copy_stations_from_s3_to_redshift = copy_stations_from_s3_to_redshift()
    
    # In this form, we can run conceptually isolated tasks in parallel.
    create_trips_table >> copy_trips_from_s3_to_redshift
    create_stations_table >> copy_stations_from_s3_to_redshift
    
redshift_dag_partitioned_tested_sla()


            
    
