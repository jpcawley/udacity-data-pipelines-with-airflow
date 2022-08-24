from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operator params
                 data_quality_checks=[],
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.data_quality_checks = data_quality_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if len(self.data_quality_checks) <= 0:
            self.log.info('No data quality checks have been provided.')
            return
        
        errors = 0
        failing_tests = []
        
        for check in self.data_quality_checks:
            qry = check.get('qry')
            result = check.get('expected_result')

            try:
                self.log.info('Executing query: {}'.format(qry))
                records = redshift_hook.get_records(qry)[0]
            except Exception as e:
                self.log.info('Query failed with exception: {}'.format(e))

            if result != records[0]:
                errors += 1
                failing_tests.append(qry)

        if errors > 0:
            self.log.info('Some tests failed.')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed.')
        else:
            self.log.info("All data quality checks passed.")
