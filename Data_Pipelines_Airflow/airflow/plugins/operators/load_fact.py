from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = f"INSERT INTO {table} {sql}"
        self.table = table
        self.redshift_conn_id = redshift_conn_id

        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         self.log.info("Clearing data from destination Redshift table")
#         redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info(f"SQL: {self.sql}")
        redshift.run(self.sql)
        
