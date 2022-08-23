from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    load_fact_qry = '''
    INSERT INTO {}
    {};
    '''

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 table='',
                 redshift_conn_id='',
                 sql_stmt='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.load_fact_qry.format(
            self.table,
            self.sql_stmt
        )
        self.log.info('Loading {} into Redshift.'.format(self.table))

        # uncomment the line below to see the insert query for the fact table
        self.log.info('Executing {}'.format(self.sql_stmt))

        redshift.run(formatted_sql)
