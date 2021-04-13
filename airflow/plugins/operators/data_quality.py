from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_query="",
                 expected_result="",
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_query = data_quality_query
        self.expected_result = expected_result
    
    def execute(self, context):
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Credentials read successfully")
        
        records = redshift_hook.get_records(self.data_quality_query)
        if records[0][0] != self.expected_result:
            raise ValueError(
            f"""
                {records[0][0]} records was found \
                 failed the quality test
            """)
        else:
            self.log.info("passed the quality test")