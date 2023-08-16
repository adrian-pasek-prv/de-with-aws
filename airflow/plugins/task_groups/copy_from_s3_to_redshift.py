from airflow.decorators import task_group
from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.s3_to_redshift_operator import S3ToRedshiftOperator

def copy_from_s3_to_redshift(
                            group_id="copy_from_s3_to_redshift_task_group",
                            redshift_conn_id="",
                            aws_credentials_id="",
                            table="",
                            create_sql_stmt="",
                            s3_bucket="",
                            s3_key="",
                            aws_region="",
                            *args, **kwargs
):
    """
    Return a TaskGroup object with specified parameters
    """
    @task_group(group_id=group_id)
    def copy_from_s3_to_redshift_task_group():
        
        create_table = PostgresOperator(task_id=f"create_{table}_table",
                                        postgres_conn_id=redshift_conn_id,
                                        sql=create_sql_stmt
            )
        
        load_to_s3 = S3ToRedshiftOperator(task_id=f"load_{table}_to_s3",
                                          redshift_conn_id=redshift_conn_id,
                                          aws_credentials_id=aws_credentials_id,
                                          s3_bucket=s3_bucket,
                                          s3_key=s3_key,
                                          table=table,
                                          aws_region=aws_region,
            )
        
        create_table >> load_to_s3
    
    return copy_from_s3_to_redshift_task_group()

    
            