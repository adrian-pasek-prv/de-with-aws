import datetime
import logging
import sys

from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from task_groups.copy_from_s3_to_redshift import copy_from_s3_to_redshift
from custom_operators.has_rows_operator import HasRowsOperator

# Import sql_statements.py
import sql_statements

@dag(
    start_date=datetime.datetime.utcnow(),
)
def redshift_dag_task_group():
    
    trips_tasks = copy_from_s3_to_redshift(group_id="trips_tasks",
                                           redshift_conn_id="redshift",
                                           aws_credentials_id="aws_credentials",
                                           create_sql_stmt=sql_statements.CREATE_TRIPS_TABLE_SQL,
                                           s3_bucket="udacity-dend",
                                           s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv",
                                           table="trips",
                                           aws_region="us-west-2",)
    
    check_trips_data_quality = HasRowsOperator(task_id="data_quality_check_trips",
                                               redshift_conn_id="redshift",
                                               table="trips",)
    
    stations_tasks = copy_from_s3_to_redshift(group_id="stations_tasks",
                                              redshift_conn_id="redshift",
                                              aws_credentials_id="aws_credentials",
                                              create_sql_stmt=sql_statements.CREATE_STATIONS_TABLE_SQL,
                                              s3_bucket="udacity-dend",
                                              s3_key="data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv",
                                              table="stations",
                                              aws_region="us-west-2",)
    
    check_stations_data_quality = HasRowsOperator(task_id="data_quality_check_stations",
                                                 redshift_conn_id="redshift",
                                                 table="stations",)
    
    location_traffic_task = PostgresOperator(task_id="calculate_location_traffic",
                                             postgres_conn_id="redshift",
                                             sql=sql_statements.LOCATION_TRAFFIC_SQL)
    
    trips_tasks >> check_trips_data_quality
    stations_tasks >> check_stations_data_quality
    check_trips_data_quality >> location_traffic_task
    check_stations_data_quality >> location_traffic_task
    
redshift_dag_task_group()