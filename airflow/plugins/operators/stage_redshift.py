from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358170'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_source="",
                 json_paths="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_source = s3_source
        self.file_type="JSON"

        self.json_paths = json_paths
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info(f"Loading data from S3 to the {self.table}")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            
        self.log.info(f"Clearing data from destination table: {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))    
        
        self.log.info(f"Copy data to the staging table: {self.table}")

        copy_query = """
                COPY {table}
                FROM '{s3_source}'
                ACCESS_KEY_ID '{access_key}'
                SECRET_ACCESS_KEY '{secret_key}'
                {file_type} '{json_paths}';
                """.format(table=self.table,
                       s3_source=self.s3_source,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       file_type=self.file_type,
                       json_paths=self.json_paths)
        self.log.info(f"copy_query {copy_query}")

        redshift.run(copy_query)
        
        self.log.info(f"Finished loading data from S3 to the {self.table}")