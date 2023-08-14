import datetime
import logging
import sys

from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.has_rows_operator import HasRowsOperator
from custom_operators.s3_to_redshift_operator import S3ToRedshiftOperator


# Import sql_statements.py
import sql_statements
    

@dag(
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2019, 1, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    # Run one dag at a time
    max_active_runs=1
)
def redshift_dag_custom_operator():
    
    create_trips_table = PostgresOperator(
        task_id="create_trips_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
    
    copy_trips_task = S3ToRedshiftOperator(
        task_id="load_trips_from_s3_to_redshift",
        table="trips",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        # Use a templated field to get the value of data_interval_start from
        s3_key="data-pipelines/divvy/partitioned/{data_interval_start.year}/{data_interval_start.month}/divvy_trips.csv",
        aws_region="us-west-2",
)
    
    data_quality_check_trips = HasRowsOperator(task_id="data_quality_check_trips",
                                               redshift_conn_id="redshift",
                                               table="trips",
    )
    
    create_stations_table = PostgresOperator(
        task_id="create_stations_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_STATIONS_TABLE_SQL,
    )
      
    copy_stations_task = S3ToRedshiftOperator(
        task_id="load_stations_from_s3_to_redshift",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend",
        s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
        table="stations",
        aws_region="us-west-2",
    )
    
    data_quality_check_stations = HasRowsOperator(task_id="data_quality_check_stations",
                                                  redshift_conn_id="redshift",
                                                  table="stations"
    )
                                                  
    
    # In this form, we can run conceptually isolated tasks in parallel.
    create_trips_table >> copy_trips_task >> data_quality_check_trips
    create_stations_table >> copy_stations_task >> data_quality_check_stations
    
redshift_dag_custom_operator()


            
    
