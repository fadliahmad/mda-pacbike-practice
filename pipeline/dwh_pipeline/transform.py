import luigi
import logging
import pandas as pd
import time
import subprocess as sp
from datetime import datetime
from pipeline.dwh_pipeline.load import Load_DWH
import os
from pipeline.utils.log import etl_log

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_DBT_TRANSFORM = os.getenv("DIR_DBT_TRANSFORM")
DIR_LOG = os.getenv("DIR_LOG")

class Transform(luigi.Task):
    
    def requires(self):
        return Load_DWH()
    
    def run(self):
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for transform tables
        start_time = time.time()
        logging.info("==================================STARTING TRANSFROM DATA=======================================")  
               
        # Transform to dimensions tables
        try:
            with open (file = f'{DIR_LOG}/logs.log', mode = 'a') as f :
                sp.run(
                    f"cd {DIR_DBT_TRANSFORM} && dbt snapshot && dbt run && dbt test",
                    stdout = f,
                    stderr = sp.PIPE,
                    text = True,
                    shell = True,
                    check = True
                )
        
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            logging.info("Transform Tables - SUCCESS")
            log_msg = {
                        "step" : "warehouse | transform",
                        "process":"transform",
                        "status": "success",
                        "source": "raw schema",
                        "table_name": None,
                        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                    }
        except Exception as e:
            logging.error(f"Transform to All Dimensions and Fact Tables - FAILED")
        
            log_msg = {
                        "step" : "warehouse | transform",
                        "process":"transform",
                        "status": f"failed",
                        "source": "raw schema",
                        "table_name": None,
                        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                        "error_msg": str(e)
                    }
            
            logging.error("Transform Tables - FAILED")  
        finally:
            etl_log(log_msg)
            pd.DataFrame([log_msg]).to_csv(f"{DIR_TEMP_LOG}/dwh_transform_done.csv", index=False)
        
        logging.info("==================================ENDING TRANSFROM DATA=======================================") 
     
    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return luigi.LocalTarget(f"{DIR_TEMP_LOG}/dwh_transform_done.csv")
        