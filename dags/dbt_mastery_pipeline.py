"""
Airflow DAG for dbt_mastery incremental data pipeline

This DAG orchestrates:
1. Detection of new CSV files
2. Loading data to MySQL raw table
3. Running dbt models incrementally (staging → intermediate → marts)
4. Testing at each layer
5. Archiving processed files

Author: Alex Austin
Created: 2025-11-14
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import sys

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))
from file_helpers import find_latest_csv, move_file, check_file_exists

# Import configuration
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'config'))
from dbt_config import (
    DBT_PROJECT_DIR, INCOMING_DATA_DIR, PROCESSED_DATA_DIR,
    MYSQL_DATABASE, MYSQL_RAW_TABLE, CSV_FILE_PATTERN,
    DBT_DEPS, DBT_RUN_STAGING, DBT_TEST_STAGING,
    DBT_RUN_INTERMEDIATE, DBT_TEST_INTERMEDIATE,
    DBT_RUN_MARTS, DBT_TEST_MARTS
)


# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'alex',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='dbt_mastery_incremental_pipeline',
    default_args=default_args,
    description='Incremental data pipeline with dbt transformations',
    schedule_interval=None,  # Manual trigger for now
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['dbt', 'incremental', 'data-pipeline'],
)


# =============================================================================
# Python Callable Functions
# =============================================================================

def check_for_new_csv(**context):
    """
    Check if new CSV file exists in incoming directory
    Returns the file path for downstream tasks
    """
    csv_file = find_latest_csv(INCOMING_DATA_DIR, CSV_FILE_PATTERN)

    if csv_file is None:
        raise FileNotFoundError(
            f"No CSV files matching '{CSV_FILE_PATTERN}' found in {INCOMING_DATA_DIR}"
        )

    # Verify file is not empty
    if not check_file_exists(csv_file):
        raise ValueError(f"File exists but is empty: {csv_file}")

    print(f"Found CSV file: {csv_file}")
    print(f"File size: {os.path.getsize(csv_file) / (1024*1024):.2f} MB")

    # Push file path to XCom for other tasks
    context['ti'].xcom_push(key='csv_file_path', value=csv_file)

    return csv_file


def load_csv_to_mysql(**context):
    """
    Load CSV file to MySQL raw table using pandas
    Incremental load (appends data)
    """
    import pandas as pd
    from pandas.compat import _optional
    from sqlalchemy import create_engine

    # pandas 2.x requires SQLAlchemy >=2.0, but Airflow pins SQLAlchemy 1.4.x.
    # Relax the version guard so pandas will use the SQLAlchemy path with 1.4.x.
    _optional.VERSIONS["sqlalchemy"] = "1.4.0"

    # Get CSV file path from previous task
    csv_file = context['ti'].xcom_pull(
        task_ids='check_new_data',
        key='csv_file_path'
    )

    print(f"Loading CSV: {csv_file}")

    # Read CSV
    print("Reading CSV file...")
    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df):,} rows from CSV")

    # Create SQLAlchemy engine
    # NOTE: @ symbol in password must be URL-encoded as %40
    engine = create_engine(
        f'mysql+pymysql://root:Alex%4012345@localhost/{MYSQL_DATABASE}'
    )

    # Append to raw table (incremental)
    # After relaxing the version guard above, pandas will treat the 1.4 SQLAlchemy
    # engine as a connectable and use its SQLAlchemy code path.
    print(f"Appending to {MYSQL_RAW_TABLE}...")
    with engine.begin() as conn:  # commit on successful exit
        df.to_sql(
            MYSQL_RAW_TABLE,
            conn,
            if_exists='append',  # Incremental load
            index=False,
            chunksize=10000  # Process in chunks for large files
        )

    print(f"Successfully loaded {len(df):,} rows to {MYSQL_RAW_TABLE}")

    return len(df)


def archive_processed_csv(**context):
    """
    Move processed CSV file to processed directory
    """
    csv_file = context['ti'].xcom_pull(
        task_ids='check_new_data',
        key='csv_file_path'
    )

    if csv_file and os.path.exists(csv_file):
        new_path = move_file(csv_file, PROCESSED_DATA_DIR)
        print(f"Moved file to: {new_path}")
        return new_path
    else:
        print("No file to archive (already processed)")
        return None


# =============================================================================
# Task Definitions
# =============================================================================

# Task 1: Check for new CSV files
check_new_data = PythonOperator(
    task_id='check_new_data',
    python_callable=check_for_new_csv,
    dag=dag,
)

# Task 2: Load CSV to MySQL
load_raw_data = PythonOperator(
    task_id='load_raw_data_to_mysql',
    python_callable=load_csv_to_mysql,
    dag=dag,
)

# Task 3: Install dbt packages
dbt_deps_task = BashOperator(
    task_id='dbt_deps',
    bash_command=DBT_DEPS,
    dag=dag,
)

# Task Group: Staging Layer
with TaskGroup('staging_layer', dag=dag) as staging_group:

    dbt_run_staging_task = BashOperator(
        task_id='run_staging_models',
        bash_command=DBT_RUN_STAGING,
        dag=dag,
    )

    dbt_test_staging_task = BashOperator(
        task_id='test_staging_models',
        bash_command=DBT_TEST_STAGING,
        dag=dag,
    )

    dbt_run_staging_task >> dbt_test_staging_task

# Task Group: Intermediate Layer
with TaskGroup('intermediate_layer', dag=dag) as intermediate_group:

    dbt_run_intermediate_task = BashOperator(
        task_id='run_intermediate_models',
        bash_command=DBT_RUN_INTERMEDIATE,
        dag=dag,
    )

    dbt_test_intermediate_task = BashOperator(
        task_id='test_intermediate_models',
        bash_command=DBT_TEST_INTERMEDIATE,
        dag=dag,
    )

    dbt_run_intermediate_task >> dbt_test_intermediate_task

# Task Group: Marts Layer
with TaskGroup('marts_layer', dag=dag) as marts_group:

    dbt_run_marts_task = BashOperator(
        task_id='run_marts_models',
        bash_command=DBT_RUN_MARTS,
        dag=dag,
    )

    dbt_test_marts_task = BashOperator(
        task_id='test_marts_models',
        bash_command=DBT_TEST_MARTS,
        dag=dag,
    )

    dbt_run_marts_task >> dbt_test_marts_task

# Task: Archive processed file
archive_csv = PythonOperator(
    task_id='archive_processed_csv',
    python_callable=archive_processed_csv,
    dag=dag,
)


# =============================================================================
# Task Dependencies (Pipeline Flow)
# =============================================================================

# Main pipeline flow
check_new_data >> load_raw_data >> dbt_deps_task >> staging_group
staging_group >> intermediate_group >> marts_group >> archive_csv


# =============================================================================
# Documentation
# =============================================================================

dag.doc_md = """
# dbt Mastery Incremental Pipeline

This DAG processes flight route data incrementally through dbt transformations.

## Workflow

1. **check_new_data**: Scans incoming directory for new CSV files
2. **load_raw_data_to_mysql**: Appends CSV data to raw_flights_data table
3. **dbt_deps**: Installs dbt packages
4. **staging_layer**: Runs and tests staging models (views)
5. **intermediate_layer**: Runs and tests intermediate models (views)
6. **marts_layer**: Runs and tests marts models (tables, incremental)
7. **archive_processed_csv**: Moves CSV to processed directory

## Manual Trigger

This DAG is configured for manual triggering. To run:
1. Place CSV file in incoming directory
2. Trigger DAG from Airflow UI
3. Monitor progress in Graph view

## Incremental Processing

- Raw table: Appends new data
- dbt marts: Uses incremental materialization (only processes new/changed records)
- Efficient for large datasets

## Monitoring

- Check logs for each task
- Failed tasks will turn red
- Downstream tasks auto-skip if upstream fails
"""
