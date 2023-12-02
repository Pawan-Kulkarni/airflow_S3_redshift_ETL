from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.helpers.sql_queries import DataQualityChecks, DataQualityExpectedResult

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        queries = [DataQualityChecks.data_quality_check1,DataQualityChecks.data_quality_check2,DataQualityChecks.data_quality_check3,DataQualityChecks.data_quality_check4,DataQualityChecks.data_quality_check5]
        expected_result = [DataQualityExpectedResult.data_quality_check1,DataQualityExpectedResult.data_quality_check2,DataQualityExpectedResult.data_quality_check3,DataQualityExpectedResult.data_quality_check4,DataQualityExpectedResult.data_quality_check5]
        data_quality_result = {}
        for check in range(len(queries)):
            result = redshift.get_records(queries[check])[0]
            expected_result_op = expected_result[check]
            if expected_result_op != result:
                test_result = [expected_result_op,result]
                data_quality_result['test{}'.format(check)] = test_result
                self.log.info('Data Quality Check number {} failed'.format(check))
            else:
                data_quality_result['test{}'.format(check)] = "Test Case Passed"
                self.log.info('Data Quality Check number {} passed'.format(check))

        self.log.info('Data Quality Checks Successfully Conducted')
