import logging
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class HasRowsOperator(BaseOperator):
    
    def __init__(self, 
                 redshift_conn_id: str,
                 table: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        table = self.table
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        
        if len(records) < 1 or len(records[0]) < 1:
            logging.error(f"Data quality check failed. '{table}' returned no results")
            raise ValueError(f"Data quality check failed. '{table}' returned no results")
    
        num_of_records = records[0][0]
        logging.info(f"Data quality on table {table} check passed with {num_of_records:,} records")
            
        