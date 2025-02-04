from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def db_connection():
    try:
        src_database = os.getenv("SRC_POSTGRES_DB")
        src_host = os.getenv("SRC_POSTGRES_HOST")
        src_user = os.getenv("SRC_POSTGRES_USER")
        src_password = os.getenv("SRC_POSTGRES_PASSWORD")
        src_port = os.getenv("SRC_POSTGRES_PORT")

        stg_database = os.getenv("STG_POSTGRES_DB")
        stg_host = os.getenv("STG_POSTGRES_HOST")
        stg_user = os.getenv("STG_POSTGRES_USER")
        stg_password = os.getenv("STG_POSTGRES_PASSWORD")
        stg_port = os.getenv("STG_POSTGRES_PORT")

        dwh_database = os.getenv("DWH_POSTGRES_DB")
        dwh_host = os.getenv("DWH_POSTGRES_HOST")
        dwh_user = os.getenv("DWH_POSTGRES_USER")
        dwh_password = os.getenv("DWH_POSTGRES_PASSWORD")
        dwh_port = os.getenv("DWH_POSTGRES_PORT")
        
        src_conn = f'postgresql://{src_user}:{src_password}@{src_host}:{src_port}/{src_database}'
        dwh_conn = f'postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_database}'
        stg_host = f'postgresql://{stg_user}:{stg_password}@{stg_host}:{stg_port}/{stg_database}'
        
        src_engine = create_engine(src_conn)
        dwh_engine = create_engine(dwh_conn)
        stg_engine = create_engine(stg_host)
        
        return src_engine, stg_engine, dwh_engine

    except Exception as e:
        print(f"Error: {e}")
        return None

def db_log_connection():
    try:
        log_database = os.getenv("LOG_POSTGRES_DB")
        log_host = os.getenv("LOG_POSTGRES_HOST")
        log_user = os.getenv("LOG_POSTGRES_USER")
        log_password = os.getenv("LOG_POSTGRES_PASSWORD")
        log_port = os.getenv("LOG_POSTGRES_PORT")

        log_conn = f'postgresql://{log_user}:{log_password}@{log_host}:{log_port}/{log_database}'
        log_engine = create_engine(log_conn)
        
        return log_engine

    except Exception as e:
        print(f"Error: {e}")
        return None