from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table = table
        self.sql_stmt=sql_stmt

    def execute(self, context):
        self.log.info('LoadFactOperator about to be implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_fact = """
        INSERT INTO {}
        {}
        """.format(self.table, self.sql_stmt)
        redshift.run(insert_fact)
        self.log.info(f'Insertion for fact is complete')
