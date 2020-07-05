from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        DATEFORMAT AS 'auto'
        TIMEFORMAT AS 'epochmillisecs'
        {}
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 table="",
                 filetype='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table=table
        self.filetype=filetype
    def execute(self, context,*args, **kwargs):
        logging.info("<<<<<<Starting Execution>>>>>")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        if (self.filetype=='json'):
            fix=" json 'auto';"
        else:
            fix=" csv  IGNOREHEADER 1;"
            
        logging.info("File Type Detected")
        logging.info(fix)
        logging.info("Clearing data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))
        logging.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            fix
        )
        logging.info("SQL Generated")
        logging.info(formatted_sql)
        redshift_hook.run(formatted_sql)
        logging.info("Data Pushed to Staging Table")
