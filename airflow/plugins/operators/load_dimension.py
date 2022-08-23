from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    load_dimension_qry = '''
    INSERT INTO {}
    {};
    '''

    truncate_qry = '''
    TRUNCATE TABLE {};
    '''

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 table='',
                 redshift_conn_id='',
                 sql_stmt='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        truncate_sql = LoadDimensionOperator.truncate_qry.format(self.table)
        if self.truncate:
            self.log.info('Truncating {} table.'.format(self.table))
            redshift.run(truncate_sql)

        self.log.info('Loading {} into Redshift.'.format(self.table))
        loading_sql = LoadDimensionOperator.load_dimension_qry.format(
            self.table,
            self.sql_stmt,
        )

        # uncomment the line below to see the insert query for the dimension table
        self.log.info('Executing {}'.format(self.sql_stmt))

        redshift.run(loading_sql)
