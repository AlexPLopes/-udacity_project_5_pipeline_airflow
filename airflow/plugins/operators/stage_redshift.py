from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
  
    ui_color = '#358140'
    
    copy_statements    = """
     COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 bucket_adress="",
                 archive_diretory="",
                 json_path="auto",
                 region="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.bucket_adress = bucket_adress
        self.archive_diretory = archive_diretory
        self.json_path = json_path
        self.region = region
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Credentials read successfully")
        
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("table cleared successfully")
        
        rendered_key = self.archive_diretory.format(**context)
        bucket_path = "s3://{}/{}".format(self.bucket_adress, rendered_key)
        s3toredshift_query = StageToRedshiftOperator.copy_statements.format(
            self.table,
            bucket_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(s3toredshift_query)
        self.log.info("Data loaded successfully")