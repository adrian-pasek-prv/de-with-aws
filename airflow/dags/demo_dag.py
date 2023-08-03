from airflow.decorators import dag, task
import logging
import datetime
import os

@dag(
    schedule='@daily',
    start_date=datetime.datetime.now() - datetime.timedelta(days=4),
    tags=['demo']
)
def demo_dag():
    
    @task()
    def hello_world():
        logging.info('Hello, World!')
        
    hello_world = hello_world()
    
    @task()
    def current_time(**kwargs):
        # **kwargs will be populated with Airflow context variables like data_interval_start 
        logging.info(f"Current time is {datetime.datetime.utcnow().isoformat()}")
        logging.info(f"data_interval_start is set to \"{kwargs['data_interval_start']}\"")
    current_time = current_time()
    
    @task()
    def working_dir():
        logging.info(f"Working directory is {os.getcwd()}")
        
    working_dir = working_dir()
    
    @task()
    
    def complete():
        logging.info("DAG completed")
        
    complete = complete()
    
    # Task dependencies should look like this:
    #
    #                -> current_time
    #               /               \
    #   hello_world                  -> complete
    #               \               /
    #                 -> working_dir
    
    hello_world >> current_time
    hello_world >> working_dir
    current_time >> complete
    working_dir >> complete
    
demo_dag()
