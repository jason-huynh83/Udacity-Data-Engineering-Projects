from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert = """
        INSERT INTO {} ({})
        """

    @apply_defaults
    # Define your operators params (with defaults) here
    # Example:
    # conn_id = your-connection-name    
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 *args, **kwargs):
        # Map params here
        # Example:
        # self.conn_id = conn_id
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.sql=sql


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_query =LoadFactOperator.insert.format(self.table, self.sql)
        self.log.info(f"loading into fact table {self.table}")
        redshift.run(insert_query)
        self.log.info(f"loaded into fact table {self.table}")        
        #self.log.info('LoadFactOperator not implemented yet')
