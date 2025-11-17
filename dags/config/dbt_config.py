
"""
Configuration for dbt_mastery Airflow pipeline
"""

# Paths
DBT_PROJECT_DIR = '/Users/alexaustinchettiar/Downloads/dbt_mastery'
DBT_PROFILES_DIR = '/Users/alexaustinchettiar/.dbt'

# Data directories
INCOMING_DATA_DIR = '/Users/alexaustinchettiar/data_pipeline/incoming'
PROCESSED_DATA_DIR = '/Users/alexaustinchettiar/data_pipeline/processed'
ARCHIVE_DATA_DIR = '/Users/alexaustinchettiar/data_pipeline/archive'

# MySQL connection (should match your dbt profile)
MYSQL_CONN_ID = 'mysql_dbt_mastery'  # Airflow connection ID
MYSQL_DATABASE = 'dbt_mastery'
MYSQL_RAW_TABLE = 'raw_flights_data'

# dbt commands
DBT_DEPS = f'cd {DBT_PROJECT_DIR} && dbt deps'
DBT_RUN_STAGING = f'cd {DBT_PROJECT_DIR} && dbt run --select staging.*'
DBT_TEST_STAGING = f'cd {DBT_PROJECT_DIR} && dbt test --select staging.*'
DBT_RUN_INTERMEDIATE = f'cd {DBT_PROJECT_DIR} && dbt run --select intermediate.*'
DBT_TEST_INTERMEDIATE = f'cd {DBT_PROJECT_DIR} && dbt test --select intermediate.*'
DBT_RUN_MARTS = f'cd {DBT_PROJECT_DIR} && dbt run --select marts.*'
DBT_TEST_MARTS = f'cd {DBT_PROJECT_DIR} && dbt test --select marts.*'

# File patterns
CSV_FILE_PATTERN = 'routes_*.csv'

# Email settings (for future alerts)
ALERT_EMAIL = 'alex@example.com'

