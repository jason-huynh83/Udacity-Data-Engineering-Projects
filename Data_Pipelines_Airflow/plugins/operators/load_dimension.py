from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert = """
        INSERT INTO {} ({});
    """

    @apply_defaults
    # Define your operators params (with defaults) here
    # Example:
    # conn_id = your-connection-name
    def __init__(self,
                 redshift_conn_id='',
                 table = '',
                 sql = '',
                 *args, **kwargs):
        
        # Map params here
        # Example:
        # self.conn_id = conn_id
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_query =LoadDimensionOperator.insert.format(self.table, self.sql)
        self.log.info(f"loading into dimension table {self.table}")
        redshift.run(insert_query)
        self.log.info(f"loaded into dimension table {self.table}")
        #self.log.info('LoadDimensionOperator not implemented yet')
