from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    Airflow Operator 'LoadDimensionOperator'
    Used to insert data from staging tables to dimention table
    
    Keyword arguments:
        redshift_conn_id ==> Redshift connection ID
        table ==> table name
        columns ==> target columns name
        query ==> Query name
        insert_mode ==> inserting mechanism (replace or update the table)
        
    """
    
    ui_color = '#80BD9E'
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

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.query = query
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info('Start Implementing LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # List of queries names and statments to be used in the for loop below
        # The porpes is to match the query name with the right statment
        queries = ["user_table_insert", "song_table_insert", "artist_table_insert", "time_table_insert"]
        queries_insert = [SqlQueries.user_table_insert, SqlQueries.song_table_insert, \
                          SqlQueries.artist_table_insert, SqlQueries.time_table_insert]
        queries_appen = [SqlQueries.user_table_append, SqlQueries.song_table_append, \
                         SqlQueries.artist_table_append, SqlQueries.time_table_append]
        
        self.log.info('Select Inserting Mechanism')
        assign_query = ""
        if self.insert_mode == 'truncate':
            self.log.info("Insert Mode = Truncate. Clean the table")
            redshift.run("DELETE FROM {}".format(self.table))
            for n in range(4):
                if self.query == queries[n]:
                    assign_query = queries_insert[n]
        elif self.insert_mode == 'append':
            self.log.info("Insert Mode = Append. Add to the existing table")
            for n in range(4):
                if self.query == queries[n]:
                    assign_query = queries_appen[n]
                    assign_query = assign_query.format(self.table, self.table)
        else:
            raise ValueError("Insert Mode Not Correct")
                    
        self.log.info("Preparing SQL query for {} table".format(self.table))
        sql_query = LoadDimensionOperator.load_sql.format(
            self.table,
            self.columns,
            assign_query
        )
        redshift.run(sql_query)
        