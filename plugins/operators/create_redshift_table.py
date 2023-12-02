from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import SqlQueries

class CreateRedshiftTable(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(CreateRedshiftTable, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Starting Tables Creation')
        redshift.run(SqlQueries.staging_events_table_create)
        redshift.run(SqlQueries.staging_songs_table_create)
        redshift.run(SqlQueries.songplay_table_create)
        redshift.run(SqlQueries.user_table_create)
        redshift.run(SqlQueries.song_table_create)
        redshift.run(SqlQueries.artist_table_create)
        redshift.run(SqlQueries.time_table_create)
        self.log.info('Tables Creation Successfull!')
