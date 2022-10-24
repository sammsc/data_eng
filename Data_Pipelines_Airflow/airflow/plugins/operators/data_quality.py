from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for check in self.checks:
            records = redshift_hook.get_records(check['test_sql'])

            self.log.info(f"Check query: {check['test_sql']}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. Query returned no results")

            num_records = records[0][0]
            comparison = str(num_records) + check['comparison'] + check['expected_result']
            if not eval(comparison):
                raise ValueError(f"Data quality check failed. Result is {num_records}, expecting {check['expected_result']}")

            self.log.info(f"Data quality check passed with {num_records} records")
    
