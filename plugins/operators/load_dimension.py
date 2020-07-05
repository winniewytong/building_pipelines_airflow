from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table = "",
                truncate = False,
                sql_stmt =  "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.truncate=truncate
        self.sql_stmt=sql_stmt

    def execute(self, context):
        """
        LoadDimensionOperator loads data from fact table to various dim tables.
        The "truncate" paramenter allows flexibility in deleting existing data
        first before further insertion.
        """
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_dims = """
        INSERT INTO {}
        {};
        """.format(self.table,self.sql_stmt)

        if self.truncate:
            insert_dims = """
            TRUNCATE TABLE {};
            INSERT INTO {}
            {};
        """.format(self.table,self.table,self.sql_stmt)

        redshift.run(insert_dims)
        self.log.info(f'Insertion for table {self.table} is complete')
