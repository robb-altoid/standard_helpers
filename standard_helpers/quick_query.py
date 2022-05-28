from typing import Union
from databricks import sql
from pyspark.shell import spark
from pyspark.sql import functions as F
import pandas as pd


def get_serverless_query(conn_info: dict, q: str, return_result: bool = True, return_type: str = "sdf", head: int = None) -> Union[F.DataFrame, None]:
    """
    Queries the specified Serverless SQL endpoint, or any Databricks SQL endpoint, to speed up your query

    Args:
    conn_info: dict
        pass the necessary information to connect to the speedy sql endpoint. dict keys:
            server_hostname
            http_path
            access_token

    q: str 
        SQL query string
        
    return_result: bool
        if true, returns the results of the query. set to false if you just want the query to be executed to 
        perform a function that doesn't return a result

    return_type: str
        specify if the data should be returned as a spark dataframe (sdf), pandas dataframe (pdf), or a list of lists (lst). default
        is a sdf

    head: int
        The maximum number of rows to return from the query. default is None (the number of rows will not be limited)

    Returns:
    q_result_out: spark.dataframe or pandas.dataframe or list
        the query results 
    """

    server_hostname = conn_info['server_hostname']
    http_path = conn_info['http_path']
    access_token = conn_info['access_token']

    with sql.connect(server_hostname=server_hostname,
                    http_path=http_path,
                    access_token=access_token) as connection:
        with connection.cursor() as cursor:
            cursor.execute(q)
            if not return_result:
                return
            
            if head:
                q_result_lst = cursor.fetchmany(head)
            else:
                q_result_lst = cursor.fetchall() 

            if return_type == "sdf":   
                # the results come back as a list, this converts to a spark.df. this will fail if the data types can't be inferred.
                # if this happens then you have to provide the column names/types (schema). see this link for details:
                # https://sparkbyexamples.com/pyspark/pyspark-create-dataframe-from-list/
                q_result_out = spark.createDataFrame(data = q_result_lst)
                return q_result_out
            
            elif return_type == "pdf":   
                q_result_out = convert_serveless_query_to_pddf(q_result_lst)
                return q_result_out
            
            else:
                q_result_out = q_result_lst
                return q_result_out


def create_table_from_sdf(db_and_tbl_name, sdf_to_convert):
    """
    creates a new table in a specified database from a spark dataframe or overwrites an existing one. deletes the table if it already exists but
    the schema is different.

    Args:
    db_and_tbl_name: str
        the name of the db and table in SQL style, for example "robb_db.vomz_transactions"
        
    sdf_to_convert: spark.DataFrame
        the dataframe containing the data to be loaded into the new database table. the column name and order
        will be preserved in the new table. the datatypes will be inferred during creation.
        
    Returns:
        nothing
    """

    # write the data to the data table
    sdf_to_convert.write.mode('overwrite').saveAsTable(f'{db_and_tbl_name}')
      

def run_qry_n_create_tbl(conn_info: dict, qry:str, db_n_tbl_name:str):
    """
    stiches together the get_serverless_qry and create_tbl_from_spd functions. input the query string
    and the name of the database.table that you want the data stored in. the functions will run the query
    and create the table. the function returns the spark.dataframe

    Args:
    conn_info: dict
        the server connection info need to pass to the get_serverless_query function

    qry: str
        the query to generate the desired data

    db_n_tbl_name:str
        the name of the database and table (database.table) to store the data in. appends the data
        to the table if it already exists. will fail if the schema is different for some reason.
        
    Returns:
        nothing
    """
    resultant_sdf = get_serverless_query(conn_info, qry)
    create_table_from_sdf(db_n_tbl_name, resultant_sdf)
    return
  

def convert_serveless_query_to_pddf(input_query_result: list):
  """ 
  get_serverless_query returns a list of rows (which are dictionaries). This function converts the data
  into a pd.DataFrame with the proper column names
  
  Attributes
    input_query_result: list
      This is the list of rows that the "get_serverless_query" return
      
  Returns
    pd_df: pd.DataFrame
      The results of the query turned into rows of a pd.DataFrame with the column names from the query
  """
  pd_df = pd.DataFrame(input_query_result)
  
  # this is a total bodge to add the column names. For some reason can't convert the query results to a dict
  # and then create the DataFrame using pd.DataFrame.from_dict(data, orient='columns')
  default_col_names = list(pd_df.columns)
  original_col_names = list(input_query_result[0].asDict().keys())
  new_col_names = dict(zip(default_col_names,original_col_names))
  pd_df = pd_df.rename(columns=new_col_names)
  return pd_df