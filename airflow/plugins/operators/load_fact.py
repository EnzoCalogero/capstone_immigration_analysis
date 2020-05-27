from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_select_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_select_query = insert_select_query

    def execute(self, context):
        self.log.info(f"Loading data from staging tables to the {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Copy data to the fact table: {self.table}")
        insert_query = """
            INSERT INTO {table}
            {insert_select_query}
        """.format(table = self.table,
                   insert_select_query = self.insert_select_query)
        redshift.run(insert_query)
        
        self.log.info(f"Finished loading data from staging tables to the {self.table}")