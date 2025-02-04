import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from pangres import upsert

from pipeline.dwh_pipeline.extract_db import Extract_STG
from pipeline.utils.read_sql import read_sql as read_sql_file
from pipeline.utils.log import etl_log
from pipeline.utils.conn import db_connection

import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load_DWH(luigi.Task):
    
    def requires(self):
        return Extract_STG()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')

        # Read Data to be load
        try:
            # Read CSV File
            df_currency = pd.read_csv(self.input()[0].path)
            df_person = pd.read_csv(self.input()[1].path)
            df_employee = pd.read_csv(self.input()[2].path)
            df_product = pd.read_csv(self.input()[3].path)
            df_shipmethod = pd.read_csv(self.input()[4].path)
            df_currencyrate = pd.read_csv(self.input()[5].path)
            df_salesreason = pd.read_csv(self.input()[6].path)
            df_salesterritory = pd.read_csv(self.input()[7].path)
            df_specialoffer = pd.read_csv(self.input()[8].path)
            df_salesperson = pd.read_csv(self.input()[9].path)
            df_store = pd.read_csv(self.input()[10].path)
            df_customer = pd.read_csv(self.input()[11].path)
            df_specialofferproduct = pd.read_csv(self.input()[12].path)
            df_salespersonquotahistory = pd.read_csv(self.input()[13].path)
            df_salesterritoryhistory = pd.read_csv(self.input()[14].path)
            df_shoppingcartitem = pd.read_csv(self.input()[15].path)
            df_salesorderheader = pd.read_csv(self.input()[16].path)
            df_salesorderdetail = pd.read_csv(self.input()[17].path)
            df_salesorderheadersalesreason = pd.read_csv(self.input()[18].path)
            
            # list of dataframes
            list_df = [df_currency,
                       df_person,
                       df_employee,
                       df_product,
                       df_shipmethod,
                       df_currencyrate,
                       df_salesreason,
                       df_salesterritory,
                       df_specialoffer,
                       df_salesperson,
                       df_store,
                       df_customer,
                       df_specialofferproduct,
                       df_salespersonquotahistory,
                       df_salesterritoryhistory,
                       df_shoppingcartitem,
                       df_salesorderheader,
                       df_salesorderdetail,
                       df_salesorderheadersalesreason]
            
            # list of dataframes index name, for upsert
            df_index_name = [{'table_name': 'currency','idx_name': 'currencycode'},
                            {'table_name': 'person', 'idx_name': 'businessentityid'},
                            {'table_name': 'employee', 'idx_name': 'businessentityid'},
                            {'table_name': 'product', 'idx_name': 'productid'},
                            {'table_name': 'shipmethod', 'idx_name': 'shipmethodid'},
                            {'table_name': 'currencyrate', 'idx_name': 'currencyrateid'},
                            {'table_name': 'salesreason', 'idx_name': 'salesreasonid'},
                            {'table_name': 'salesterritory', 'idx_name': 'territoryid'},
                            {'table_name': 'specialoffer', 'idx_name': 'specialofferid'},
                            {'table_name': 'salesperson', 'idx_name': 'businessentityid'},
                            {'table_name': 'store', 'idx_name': 'businessentityid'},
                            {'table_name': 'customer', 'idx_name': 'customerid'},
                            {'table_name': 'specialofferproduct', 'idx_name': ['specialofferid', 'productid']},
                            {'table_name': 'salespersonquotahistory', 'idx_name': ['businessentityid', 'quotadate']},
                            {'table_name': 'salesterritoryhistory', 'idx_name': ['businessentityid', 'startdate', 'territoryid']},
                            {'table_name': 'shoppingcartitem', 'idx_name': 'shoppingcartitemid'},
                            {'table_name': 'salesorderheader', 'idx_name': 'salesorderid'},
                            {'table_name': 'salesorderdetail', 'idx_name': ['salesorderid', 'salesorderdetailid']},
                            {'table_name': 'salesorderheadersalesreason', 'idx_name': ['salesorderid', 'salesreasonid']}]
            
            
            logging.info(f"Read Extracted Data - SUCCESS")
            
        except Exception:
            logging.error(f"Read Extracted Data  - FAILED")
            raise Exception("Failed to Read Extracted Data")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH Schema: Raw
        try:
            _, _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH Schema: Raw - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH Schema: Raw - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for loading tables
        start_time = time.time()  
        full_log = []
        logging.info("==================================STARTING LOAD DATA TO DWH Schema: Raw=======================================")

        # Load to tables to dvdrental schema
        for i, df in enumerate(list_df):
            table_name = df_index_name[i]['table_name']
            source = 'staging'
            try:
                # set data index or primary key
                df = df.set_index(df_index_name[i]['idx_name'])
                df['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Do upsert (Update for existing data and Insert for new data)
                upsert(con = dwh_engine,
                        df = df,
                        table_name = table_name,
                        schema = 'raw',
                        if_row_exists = "update")
            
                #create success log message
                log_msg = {
                        "step" : "warehouse | raw",
                        "process":"load",
                        "status": "success",
                        "source": source,
                        "table_name": table_name,
                        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
                    }
                logging.info(f"LOAD DWH Schema: Raw '{table_name}' - SUCCESS.")
                full_log.append(log_msg)
            except Exception as e:
                logging.error(f"EXTRACT '{df_index_name[0]['table_name']}' - FAILED.")  
                #create failed log message
                log_msg = {
                        "step" : "warehouse | raw",
                        "process":"load",
                        "status": "failed",
                        "source": source,
                        "table_name": table_name,
                        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
                        "error_msg": str(e)
                    }
                logging.error(f"Read Extracted Data  - FAILED")
                raise Exception("Failed to Read Extracted Data")
            finally:
                 etl_log(log_msg)


            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            pd.DataFrame(full_log).to_csv(f"{DIR_TEMP_LOG}/dwh_raw_load_done.csv", index=False)
        logging.info("==================================ENDING LOAD DATA TO DWH Schema: Raw=======================================")
        
    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return luigi.LocalTarget(f"{DIR_TEMP_LOG}/dwh_raw_load_done.csv")
        