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
import requests

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_LOG = os.getenv("DIR_LOG")
API_LINK = os.getenv("API_LINK")

class Extract_API(luigi.Task):
    data_name = luigi.Parameter()

    def requires(self):
        pass

    def run(self):        
        try:
            # Configure logging
            logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
            
            start_time = time.time()  # Record start time
            logging.info(f"==================================STARTING EXTRACT DATA API=======================================")


            # Get date from previous process
            filter_log = {"step_name": "staging",
                    "table_name": self.data_name,
                    "status": "success",
                    "process": "load"}
            etl_date = read_etl_log(filter_log)

            # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
            # Otherwise, retrieve data added since the last successful extraction (etl_date).
            if(etl_date['max'][0] == None):
                etl_date = '1111-01-01'
            else:
                etl_date = etl_date[max][0]

            # Get data from API
            data_json = requests.get(API_LINK).json()
            df = pd.DataFrame(data_json)

            # filter data based on etl_date
            df['modifieddate'] = pd.to_datetime(df['modifieddate'])
            df = df[df['modifieddate'] > etl_date]

            # Write DataFrame to CSV
            df.to_csv(f"{DIR_TEMP_DATA}/stg/{self.data_name}.csv", index=False)
                    
            logging.info(f"EXTRACT '{self.data_name}' - SUCCESS.")
            log_msg = {
                "step" : "staging",
                "process":"extraction",
                "status": "success",
                "source": "api",
                "table_name": self.data_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                }

            logging.info(f"Extract All Data API From Sources - SUCCESS")
            
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
               
        except Exception as e:
            logging.error(f"EXTRACT '{self.data_name}' - FAILED.")  
            log_msg = {
            "step" : "staging",
            "process":"extraction",
            "status": "failed",
            "source": "database",
            "table_name": self.data_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
            }
            raise Exception(f"Failed to extract '{self.data_name}' tables")
                    
        finally:
            etl_log(log_msg)
        
        logging.info("==================================ENDING EXTRACT DATA=======================================")


                
    def output(self):
        outputs = []
        outputs.append(luigi.LocalTarget(f'{DIR_TEMP_DATA}/stg/{self.data_name}.csv'))
        return outputs