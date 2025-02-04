import luigi
from datetime import datetime
import logging
import time
import pandas as pd
from pipeline.utils.conn import db_connection
from pipeline.utils.read_sql import read_sql as read_sql_file
from pipeline.utils.log import read_etl_log
from pipeline.utils.log import etl_log
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Extract_DB(luigi.Task):
    
    # Define tables to be extracted from db sources
    tables_to_extract = [
        ['person', 'person'],
        ['humanresources', 'employee'],
        ['production', 'product'],
        ['purchasing', 'shipmethod'],
        ['sales', 'currencyrate'],
        ['sales', 'salesreason'],
        ['sales', 'salesterritory'],
        ['sales', 'specialoffer'],
        ['sales', 'salesperson'],
        ['sales', 'store'],
        ['sales', 'customer'],
        ['sales', 'specialofferproduct'],
        ['sales', 'salespersonquotahistory'],
        ['sales', 'salesterritoryhistory'],
        ['sales', 'shoppingcartitem'],
        ['sales', 'salesorderheader'],
        ['sales', 'salesorderdetail'],
        ['sales', 'salesorderheadersalesreason']
    ]
    
    def requires(self):
        pass


    def run(self):        
        try:
            # Configure logging
            logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
            
            # Define db connection engine
            src_engine, _ , _= db_connection()
            
            # Define the query using the SQL content
            extract_query = read_sql_file(
                file_path = f'{DIR_EXTRACT_QUERY}/all-tables.sql'
            )
            
            start_time = time.time()  # Record start time
            logging.info("==================================STARTING EXTRACT DATA=======================================")

                
            for index, table_name in enumerate(self.tables_to_extract):
                try:
                     # Get date from previous process
                    filter_log = {"step_name": "staging",
                            "table_name": table_name[1],
                            "status": "success",
                            "process": "load"}
                    etl_date = read_etl_log(filter_log)

                    # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
                    # Otherwise, retrieve data added since the last successful extraction (etl_date).
                    if(etl_date['max'][0] == None):
                        etl_date = '1111-01-01'
                    else:
                        etl_date = etl_date[max][0]

                    # Read data into DataFrame
                    formatted_query = extract_query.format(schema_name=table_name[0], table_name=table_name[1])
                    df = pd.read_sql_query(
                                            formatted_query,
                                            src_engine,
                                            params=(etl_date,)
                                        )

                    # Write DataFrame to CSV
                    df.to_csv(f"{DIR_TEMP_DATA}/stg/{table_name[1]}.csv", index=False)
                    
                    logging.info(f"EXTRACT '{table_name[1]}' - SUCCESS.")
                    log_msg = {
                        "step" : "staging",
                        "process":"extraction",
                        "status": "success",
                        "source": "database",
                        "table_name": table_name[1],
                        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                        }
                    
                except Exception as e:
                    logging.error(f"EXTRACT '{table_name[1]}' - FAILED.")  
                    log_msg = {
                    "step" : "staging",
                    "process":"extraction",
                    "status": "failed",
                    "source": "database",
                    "table_name": table_name[1],
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                    "error_msg": str(e)
                    }
                    raise Exception(f"Failed to extract '{table_name[1]}' tables")
                    
                finally:
                    etl_log(log_msg)

            logging.info(f"Extract All Tables From Sources - SUCCESS")
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
                    
        except Exception as e:   
            logging.info(f"Extract All Tables From Sources - FAILED")
            
            # Write exception
            raise Exception(f"FAILED to execute EXTRACT TASK !!!")
        
        logging.info("==================================ENDING EXTRACT DATA=======================================")


                
    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'{DIR_TEMP_DATA}/stg/{table_name[1]}.csv'))
            
        return outputs