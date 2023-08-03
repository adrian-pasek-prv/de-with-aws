import datetime
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook

@dag(
    start_date=datetime.datetime.now()
)
def log_s3_keys():
    
    @task()
    def list_s3_keys():
        # Create a hook to S3 using "aws_credentials" conne 
        hook = S3Hook(aws_conn_id="aws_credentials")
        # Get the bucket name from the "s3_bucket" Airflow variable
        bucket = Variable.get("s3_bucket")
        # Get the prefix from the "s3_prefix" Airflow variable
        prefix = Variable.get("s3_prefix")
        # Get the list of keys in the bucket
        logging.info(f"Listing Keys in bucket: \"{bucket}/{prefix}\"")
        keys = hook.list_keys(bucket, prefix=prefix)
        # Interate over S3 keys
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
    
    list_s3_keys = list_s3_keys()
    
log_s3_keys()
    
