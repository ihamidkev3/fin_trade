"""
Financial Trading Data Pipeline DAG

This DAG orchestrates the end-to-end ETL process for financial trading data:
1. Setup and Validation:
   - Configures logging for pipeline monitoring
   - Performs preflight checks (database connections, file system, env vars)
2. Extract-Load:
   - Runs the main EL pipeline script for data ingestion
3. Transform:
   - Executes dbt transformations and tests

Requirements:
    - Airflow Variables must be configured for database credentials
    - DBT project and profiles directories must be accessible
    - SQL Server and required Python packages must be installed
    - Proper permissions for database access and file operations

Schedule: Daily at 1 AM (schedule_interval='0 1 * * *')
Owner: airflow
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import sys

# Import the separated functions
from utils.logging_setup import setup_logging
from utils.preflight_checks import preflight_check

# Add project root to Python path for imports
sys.path.append(Variable.get('DBT_PROJECT_DIR', '../fin_trade'))

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Environment Variables Configuration
env_vars = {
    # Project Directories
    'DBT_PROJECT_DIR': Variable.get('DBT_PROJECT_DIR', '../fin_trade'),
    'DBT_PROFILES_DIR': Variable.get('DBT_PROFILES_DIR', '../fin_trade'),
    
    # SQL Server Configuration
    'SQL_SERVER_DRIVER': Variable.get('SQL_SERVER_DRIVER', 'ODBC Driver 17 for SQL Server'),
    'SQL_SERVER_IP': Variable.get('SQL_SERVER_IP'),
    'SQL_SERVER_PORT': Variable.get('SQL_SERVER_PORT'),
    'SQL_SERVER_ENCRYPT': Variable.get('SQL_SERVER_ENCRYPT', 'false'),
    'SQL_SERVER_TRUST_CERT': Variable.get('SQL_SERVER_TRUST_CERT', 'true'),
    'SQL_COMMAND_TIMEOUT': Variable.get('SQL_COMMAND_TIMEOUT', '300'),
    'SQL_RETRIES': Variable.get('SQL_RETRIES', '3'),
    
    # DBT Configuration
    'DBT_USER': Variable.get('DBT_USER'),
    'DBT_PASSWORD': Variable.get('DBT_PASSWORD'),
    'TARGET_SCHEMA': Variable.get('TARGET_SCHEMA'),
    'TARGET_DB_SCHEMA': Variable.get('TARGET_DB_SCHEMA'),
    'DBT_TARGET': Variable.get('DBT_TARGET', 'dev'),
}

# Path to Python scripts
SCRIPT_EL = f"python {env_vars['DBT_PROJECT_DIR']}/scripts/elt/el_pipeline.py"

with DAG(
    dag_id='fin_trade_pipeline',
    default_args=default_args,
    description='Financial trading data pipeline with Extract-Load and dbt transformations',
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['dbt', 'financial-data', 'trading', 'el'],
) as dag:

    start = EmptyOperator(task_id='start_pipeline')
    
    # Group logging and preflight tasks
    with TaskGroup(group_id='setup_and_validation') as setup_group:
        setup_logging_task = PythonOperator(
            task_id='setup_logging',
            python_callable=setup_logging,
            provide_context=True,
            op_kwargs={'env_vars': env_vars}
        )
        
        preflight_check_task = PythonOperator(
            task_id='preflight_check',
            python_callable=preflight_check,
            provide_context=True,
            op_kwargs={'env_vars': env_vars}
        )
        
        # Set dependencies within the group
        setup_logging_task >> preflight_check_task

    run_el = BashOperator(
        task_id='run_el_pipeline',
        bash_command=SCRIPT_EL,
        cwd=env_vars['DBT_PROJECT_DIR'],
        env=env_vars,
    )

    dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f"dbt deps --project-dir {env_vars['DBT_PROJECT_DIR']} --profiles-dir {env_vars['DBT_PROFILES_DIR']}",
    env=env_vars,
    )

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command=f"dbt build --project-dir {env_vars['DBT_PROJECT_DIR']} --profiles-dir {env_vars['DBT_PROFILES_DIR']}",
        env=env_vars,
    )

    dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
        bash_command=f"dbt docs generate --project-dir {env_vars['DBT_PROJECT_DIR']} --profiles-dir {env_vars['DBT_PROFILES_DIR']}",
        env=env_vars,
    )

    end = EmptyOperator(task_id='end_pipeline')

    # Define task dependencies
    start >> setup_group >> run_el >> dbt_deps >> dbt_build >> dbt_docs_generate >> end
