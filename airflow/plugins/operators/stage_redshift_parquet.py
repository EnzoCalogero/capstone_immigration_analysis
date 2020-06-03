from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperatorparque(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_source="",
                 *args, **kwargs):
        super(StageToRedshiftOperatorparque, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_source = s3_source
        self.file_type="CSV"
      

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
            IAM_ROLE 'arn:aws:iam::990386341459:role/dwhRole'
            {copy_options}
            """.format(table=self.table,
                       s3_source=self.s3_source,
                       
                       copy_options="FORMAT AS PARQUET")
        self.log.info(f"copy_query {copy_query}")

        redshift.run(copy_query)
        
        self.log.info(f"Finished loading data from S3 to the {self.table}")