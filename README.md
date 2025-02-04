# ELT With Python
## How to use this project?
1. Requirements
2. Preparations
3. Run ELT Pipeline

### 1. Requirements
- OS :
    - Linux
    - WSL (Windows Subsystem For Linux)
- Tools :
    - Dbeaver
    - Docker
- Programming Language :
    - Python
    - SQL
- Python Libray :
    - Pandas

### 2. Preparations
- **Clone repo** :
  ```
  git clone https://github.com/fadliahmad/mda-pacbike-practice.git
  ```
  
- In project directory, **create and use virtual environment**.
- In virtual environment, **install requirements** :
  ```
  pip install -r requirements.txt
  ```

- **Create env file** in project root directory :
  ```
# Source
SRC_POSTGRES_DB=
SRC_POSTGRES_HOST=
SRC_POSTGRES_USER=
SRC_POSTGRES_PASSWORD=
SRC_POSTGRES_PORT=

# STG
STG_POSTGRES_DB=
STG_POSTGRES_HOST=
STG_POSTGRES_USER=postgres
STG_POSTGRES_PASSWORD=123
STG_POSTGRES_PORT=5438

# DWH
DWH_POSTGRES_DB=
DWH_POSTGRES_HOST=
DWH_POSTGRES_USER=
DWH_POSTGRES_PASSWORD=
DWH_POSTGRES_PORT=

# LOG
LOG_POSTGRES_DB=
LOG_POSTGRES_HOST=
LOG_POSTGRES_USER=
LOG_POSTGRES_PASSWORD=
LOG_POSTGRES_PORT=

# API
API_LINK="https://api-currency-five.vercel.app/api/currencydata"

# SENTRY DSN
SENTRY_DSN=

# DIRECTORY
# Adjust with your directory. make sure to write full path
DIR_ROOT_PROJECT=""
DIR_TEMP_LOG="../temp\summary"
DIR_TEMP_DATA="...\pipeline/temp\data"
DIR_EXTRACT_QUERY="...\pipeline\model\extract"
DIR_LOAD_QUERY="...\pipeline\model\load"
DIR_TRANSFORM_QUERY="...\pipeline\model\transform"
DIR_LOG_QUERY="...\pipeline\model\log" 
DIR_LOG="...\logs"
DIR_DBT_TRANSFORM="...\pipeline\dwh_pipeline/transform/transform_pacbikes"
  ```

- **Run Data Sources & Data Warehouses** :
  ```
  docker compose up -d
  ```

### 3. Run ELT Pipeline
- python -m venv venv
- venv\Scripts\activate
- pip install -r requirements.txt
- dbt deps && dbt seed 
- Run ELT Pipeline using main script :
  ```
  python3 elt_main.py
  ```