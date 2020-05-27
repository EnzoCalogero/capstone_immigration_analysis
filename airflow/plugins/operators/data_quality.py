from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    tables = ('artists', 'users', 'songs', 'time', 'songplays')

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id=None,
        tables=None,
        list_tests=None,
        *args,
        **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self._redshift_conn_id = redshift_conn_id
        self._tables = tables
        self._list_tests = list_tests

    def check_invalid_params(self):

        # Checks if the Redshift connection identifier is valid.
        if self._redshift_conn_id is None \
                or not isinstance(self._redshift_conn_id, str) \
                or self._redshift_conn_id.strip() == '':
            raise ValueError('The Redshift connection identifier cannot be null or empty.')

        # Checks if the tables tuple is valid.
        if self._tables is None  or not isinstance(self._tables, tuple)  or len(self._tables) == 0:
            raise ValueError('The table cannot be null or empty.')
        
        for table in self._tables:
            if not isinstance(table, str) or table.strip() == '' or table not in self.tables:
                message = 'Available values for the tables tuple: {}'
                raise ValueError(message.format(', '.join(self.tables)))

    def execute(self, context):
        # Validates the operator parameteres.
        self.log.info(f"list tests: {self._list_tests}")
        self.check_invalid_params()
        postgres = PostgresHook(postgres_conn_id=self._redshift_conn_id)
        # Test are the tables populated
        for table in self._tables:
            query = 'SELECT COUNT(*) FROM {}'.format(table)
            self.log.info(query)
            records = postgres.get_records(query)
            self.log.info(f"record# { records} in the table {table}")
            if len(records) == 0 or len(records[0]) == 0 or records[0][0] == 0:
                message = 'The table {} has not passed the data quality check.'.format(table)
                self.log.error(message)
                raise ValueError(message)
            self.log.info('Data quality  for {} check with {} records.'.format(table, records[0][0]))
        # Tests have the table the extepected value?
        for query, expected_value  in self._list_tests.items():
            self.log.info(f"q  {query}")
            self.log.info(f"results {expected_value}")
            record = postgres.get_records(query)
            self.log.info(f"record -> {record[0][0]}")
            if int(record[0][0]) != int(expected_value):
                message = 'The query {} has not passed the data quality check. #record {} instead of {}'.format(query, record, expected_value)
                self.log.error(message)
                raise ValueError(message)                
            else:
                 message = 'The query {} has passed the data quality check. '.format(query)
                 self.log.error(message)
            
            
    

