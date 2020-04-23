from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140',
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    # Define your operators params (with defaults) here
    # Example:
    # redshift_conn_id=your-connection-name
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_paths='',
                 *args, **kwargs):
        # Map params here
        # Example:
        # self.conn_id = conn_id
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_paths=json_paths
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        #self.log.info('StageToRedshiftOperator not implemented yet')
        #self.log.info("Clearing data from destination Redshift table")
        #redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from S3 to Redshift")
        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)         
        if self.json_paths:
            json_paths = f's3://{self.s3_bucket}/{self.json_paths}'
        else:
            json_paths = 'auto'
            
        self.log.info('Coping data from {} to {} on table Redshift'.format(s3_path, self.table))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_paths
        )
        redshift.run(formatted_sql)



