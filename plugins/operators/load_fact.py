from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Airflow Operator 'LoadFactOperator'
    Used to insert data from staging tables to the fact table
    
    Keyword arguments:
        redshift_conn_id ==> Redshift connection ID
        table ==> table name
        columns ==> target columns name
        query ==> Query name
        insert_mode ==> inserting mechanism (replace or update the table)
        
    """
    
    ui_color = '#F98866'
    load_sql = """
        INSERT INTO {} 
        ({}) 
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table="",
                 columns='',
                 query='',
                 insert_mode='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.query = query
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info('Start Implementing LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Select Inserting Mechanism')
        assign_query = ""
        if self.insert_mode == 'truncate':
            self.log.info("Insert Mode = Truncate. Clean the table")
            redshift.run("DELETE FROM {}".format(self.table))
            assign_query = SqlQueries.songplay_table_insert
        elif self.insert_mode == 'append':
            self.log.info("Insert Mode = Append. Add to the existing table")
            assign_query = SqlQueries.songplay_table_append
            assign_query = assign_query.format(self.table, self.table)
        else:
            raise ValueError("Insert Mode Not Correct")
            
        self.log.info("Preparing SQL query for {} table".format(self.table)) 
        sql_query = LoadFactOperator.load_sql.format(
            self.table,
            self.columns,
            assign_query
        )
        redshift.run(sql_query)
