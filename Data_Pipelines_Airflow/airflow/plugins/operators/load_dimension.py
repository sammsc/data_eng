from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = f"INSERT INTO {table} {sql}"
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.append = append

        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info(f"SQL: {self.sql}")
        redshift.run(self.sql)
        
        self.log.info('LoadDimensionOperator done!')