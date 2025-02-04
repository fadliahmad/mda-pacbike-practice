import luigi
import sentry_sdk
import pandas as pd
import os

from pipeline.stg_pipeline.extract_db import Extract_DB
from pipeline.stg_pipeline.extract_api import Extract_API
from pipeline.stg_pipeline.load import Load_STG
from pipeline.dwh_pipeline.extract_db import Extract_STG as Extract_STG
from pipeline.dwh_pipeline.load import Load_DWH as Load_DWH
from pipeline.dwh_pipeline.transform import Transform as Transform
from pipeline.utils.copy_log import copy_log
from pipeline.utils.delete_temp_data import delete_temp

# Read env variables
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOG = os.getenv("DIR_LOG")
SENTRY_DSN = os.getenv("SENTRY_DSN")

# Track the error using sentry
sentry_sdk.init(
    dsn = f"{SENTRY_DSN}"
)


# Execute the functions when the script is run
if __name__ == "__main__":
    # Build the task
    luigi.build([
                Extract_DB(),
                Extract_API(data_name = "currency"),
                Load_STG(),
                Extract_STG(),
                Load_DWH(),
                Transform()])
    
    
    # Delete temp data
    delete_temp(
        directory = f'{DIR_TEMP_DATA}/dwh'
    )
    delete_temp(
        directory = f'{DIR_TEMP_DATA}/stg'
    )

    delete_temp(
        directory = f'{DIR_TEMP_LOG}'
    )