from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperatorcsv(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_source="",
                 csv_paths="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageToRedshiftOperatorcsv, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_source = s3_source
        self.file_type="CSV"

        self.json_paths = csv_paths
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info(f"csv version Loading data from S3 to the {self.table}")
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
            IGNOREHEADER {headers}
            DELIMITER '{delimiter}'
            """.format(table=self.table,
                       s3_source=self.s3_source,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       headers=self.ignore_headers,
                       delimiter=self.delimiter)
        self.log.info(f"copy_query {copy_query}")

        redshift.run(copy_query)
        
        self.log.info(f"Finished loading data from S3 to the {self.table}")