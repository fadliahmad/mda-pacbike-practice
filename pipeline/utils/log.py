import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import sqlalchemy
from pangres import upsert
import os

from pipeline.utils.conn import db_log_connection
from pipeline.utils.read_sql import read_sql

DIR_LOG = os.getenv("DIR_LOG_QUERY")

def etl_log(log_msg: dict):
    """
    This function is used to save the log message to the database.
    """
    try:
        # create connection to database
        conn = db_log_connection()

        # convert dictionary to dataframe
        df_log = pd.DataFrame([log_msg])

        #extract data log
        df_log.to_sql(name = "etl_log",  # Your log table
                        con = conn,
                        if_exists = "append",
                        index = False)
    except Exception as e:
        print("Can't save your log message. Cause: ", str(e))

def read_etl_log(filter_params: dict):
    """
    This function read_etl_log that reads log information from the etl_log table and extracts the maximum etl_date for a specific process, step, table name, and status.
    """
    try:
        # create connection to database
        conn = db_log_connection()

        # To help with the incremental process, get the etl_date from the relevant process
        """
        SELECT MAX(etl_date)
        FROM etl_log "
        WHERE
            step = %s and
            table_name ilike %s and
            status = %s and
            process = %s
        """

        query = sqlalchemy.text(read_sql(f"{DIR_LOG}\log.sql"))

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(filter_params,))

        #return extracted data
        return df
    except Exception as e:
        print("Can't execute your query. Cause: ", str(e))