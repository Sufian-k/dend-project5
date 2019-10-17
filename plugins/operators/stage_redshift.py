from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Airflow Operator 'StageToRedshiftOperator'
    Used to copy data from S3 bucket to staging tables
    
    Keyword arguments:
        redshift_conn_id ==> Redshift connection ID
        aws_credentials_id ==> S3 credentials
        table ==> table name
        s3_bucket ==> S3 bucket name
        s3_key ==> S3 data distination
        execution_date ==> the dag execution date
    
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key", "execution_date")
    copy_sql = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 execution_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.execution_date = execution_date

    def execute(self, context):
        self.log.info('Start Implementing StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        exec_date = self.execution_date.format(**context)
        self.log.info("Execution_date: {}".format(exec_date))
        
        self.log.info("Cleaning data from {} table before loading".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 bucket to {} table".format(self.table))
        rendered_key = self.s3_key.format(**context)
        s3_path = "\'s3://{}/{}\'".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(formatted_sql)