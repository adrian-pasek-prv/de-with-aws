from airflow.decorators import dag, task
import logging
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime.now(),
    catchup=False,
    tags=['demo']
)
def demo_dag():
    @task()
    def log_hello_world():
        logging.info('Hello, World!')
        
    log_hello_world = log_hello_world()

demo_dag()
