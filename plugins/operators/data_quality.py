from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):
    """
    Airflow Operator 'DataQualityOperator'
    Used to check the quality of the analytics tables
    
    Keyword arguments:
        redshift_conn_id ==> Redshift connection ID
        tables ==> tables names
        
    """
    
    has_rows = ("""
        SELECT COUNT(*)
        FROM {}
    """)
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Start Implementing DataQualityOperator')
        redshift = PostgresHook(self.redshift_conn_id)
        
        queries_check = [SqlQueries.songplay_table_check, SqlQueries.user_table_check, \
                         SqlQueries.song_table_check, SqlQueries.artist_table_check, \
                         SqlQueries.time_table_check]
        tabl = self.tables
        
        self.log.info("Preparing Data Quality Check")
        for n in range(5):
            self.log.info("Check rows for {} table".format(tabl[n]))
            query = DataQualityOperator.has_rows
            records = redshift.get_records(query.format(tabl[n]))
            self.log.info(f"RESULTS: {records[0][0]}")
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {tabl[n]} returned no rows")
            self.log.info(f"Data quality check on table {tabl[n]} passed")
            
            self.log.info("Check NULLs for {} table".format(tabl[n]))
            query = queries_check[n]
            records = redshift.get_records(query.format(tabl[n]))
            self.log.info(f"RESULTS: {records[0][0]}")
            if records[0][0] > 0:
                raise ValueError(f"Data quality check failed. {tabl[n]} returned NULL values")
            self.log.info(f"Data quality check on table {tabl[n]} passed with no NULL values")