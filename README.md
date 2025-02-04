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
  SRC_POSTGRES_DB=...
  SRC_POSTGRES_HOST=...
  SRC_POSTGRES_USER=...
  SRC_POSTGRES_PASSWORD=...
  SRC_POSTGRES_PORT=...

  # DWH
  DWH_POSTGRES_DB=...
  DWH_POSTGRES_HOST=...
  DWH_POSTGRES_USER=...
  DWH_POSTGRES_PASSWORD=...
  DWH_POSTGRES_PORT=...

  # DIRECTORY
  DIR_ROOT_PROJECT=...
  DIR_TEMP_DATA=...
  DIR_EXTRACT_QUERY=...
  DIR_LOAD_QUERY=...
  DIR_TRANSFORM_QUERY=...
  ```

- **Run Data Sources & Data Warehouses** :
  ```
  docker compose up -d
  ```

### 3. Run ELT Pipeline
- python -m venv venv
- venv\Scripts\activate
- pip install -r requirements.txt
-  dbt deps, dbt run, dbt snapshots, dbt test
- Run ELT Pipeline using main script :
  ```
  python3 elt_main.py
  ```
  