from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_query = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 table = "",
                 truncate = False,
                 ignore_headers=1,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.truncate = truncate
        self.table = table
        self.ignore_headers = ignore_headers

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate == True:
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info('Table {} Truncated'.format(self.table))

        redshift.run(self.insert_query.format(self.table, self.query))
        self.log.info('Data Inserted Successfully into {}'.format(self.table))

