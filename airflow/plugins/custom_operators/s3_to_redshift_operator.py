from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class S3ToRedshiftOperator(BaseOperator):
    # What field will be templateable, in what field can I use Airflow context variables
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        REGION '{}'
    """


    def __init__(self,
                 redshift_conn_id: str,
                 aws_credentials_id: str,
                 table: str,
                 s3_bucket: str,
                 s3_key: str,
                 delimiter: str = ",",
                 ignore_headers: int = 1,
                 aws_region: str = "us-west-2",
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.aws_region = aws_region

    def execute(self, context):
        aws_hook = AwsGenericHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter,
            self.aws_region
        )
        redshift.run(formatted_sql)
