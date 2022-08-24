from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    # make s3_key templatable i.e. context variables will render the template for example execution_date.year added to s3_key in dag
    template_fields = ('s3_key',)
    ui_color = '#358140'

    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION AS '{}'
    {};
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params
                 table='',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',
                 extra_params='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.extra_params = extra_params

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table.')
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info(f'Copying data from S3 to Redshift {self.table} table.')
        rendered_key = self.s3_key.format(**context)
        self.log.info(f'Rendered key: {rendered_key}')
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.extra_params
        )

        self.log.info(
            f'Executing copy query from "{s3_path}" to "{self.table}".')
        redshift.run(formatted_sql)
