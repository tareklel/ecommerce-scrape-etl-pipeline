from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks
        
    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        for i, check in enumerate(self.checks):
            self.log.info(f'executing quality check for {check}')
            result = redshift_hook.get_first(check['test_sql'])[0]
            if check['comparison'] == '=':
                if result != check['expected_result']:
                    raise Exception(f'Check {i} failed. Result does not match expected result')
            if check['comparison'] == '>':
                if result <= check['expected_result']:
                    raise Exception(f'Check {i} failed. Result is less than expected result')
            if check['comparison'] == '<':
                if result >= check['expected_result']:
                    raise Exception(f'Check {i} failed. Result is more than expected result')
