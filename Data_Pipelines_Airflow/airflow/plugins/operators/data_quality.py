from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 result=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.result = result
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift_hook.get_records(self.sql)
        
        self.log.info(f"Check query: {self.sql}")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. Query returned no results")
            
        num_records = records[0][0]
        if num_records != self.result:
            raise ValueError(f"Data quality check failed. Result is {num_records}, expecting {self.result}")
            
        self.log.info(f"Data quality check passed with {num_records} records")
    
#         connection = redshift.get_conn()
#         cursor = connection.cursor()
        
#         self.log.info(f"SQL: {self.sql}")
#         cursor.execute(self.sql)
#         sources = cursor.fetchall()
#         for source in sources:
#             self.log.info(f"Source: {source}")