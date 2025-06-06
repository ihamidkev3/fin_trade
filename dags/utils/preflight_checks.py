"""
Preflight check utilities for the Financial Trading pipeline
"""
from airflow.exceptions import AirflowException
from scripts.utils.logger import LOGGER
import os
import sqlalchemy

def preflight_check(**context):
    """
    Verify required resources and configurations before pipeline execution.
    Uses existing project configuration and environment variables.
    """
    env_vars = context['env_vars']
    LOGGER.info("Starting preflight checks...")
    
    # 1. File System Checks
    LOGGER.info("Checking file system resources...")
    
    required_paths = {
        "DBT project directory": f"{env_vars['DBT_PROJECT_DIR']}",
        "DBT profiles.yml": f"{env_vars['DBT_PROFILES_DIR']}/profiles.yml",
        "DBT models directory": f"{env_vars['DBT_PROJECT_DIR']}/models",
        "EL pipeline script": f"{env_vars['DBT_PROJECT_DIR']}/scripts/elt/el_pipeline.py"
    }
    
    for name, path in required_paths.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"{name} not found at {path}")
        LOGGER.info(f"✓ {name} exists: {path}")
    
    # 2. Database Connectivity Check
    LOGGER.info("Checking SQL Server connectivity...")
    
    try:
        sqlserver_url = (
            f"mssql+pyodbc://{env_vars['DBT_USER']}:{env_vars['DBT_PASSWORD']}"
            f"@{env_vars['SQL_SERVER_IP']}:{env_vars['SQL_SERVER_PORT']}/{env_vars['TARGET_DB_SCHEMA']}"
            f"?driver={env_vars['SQL_SERVER_DRIVER']}"
            f"&encrypt={env_vars['SQL_SERVER_ENCRYPT']}"
            f"&TrustServerCertificate={env_vars['SQL_SERVER_TRUST_CERT']}"
        )
        engine = sqlalchemy.create_engine(sqlserver_url)
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        LOGGER.info("✓ SQL Server connection successful")
    except Exception as e:
        LOGGER.error(f"SQL Server connection failed: {str(e)}")
        raise AirflowException(f"SQL Server connectivity check failed: {str(e)}")
    
    # 3. Environment Variable Check
    LOGGER.info("Validating environment variables...")
    
    required_vars = [
        'SQL_SERVER_IP', 'SQL_SERVER_PORT', 'DBT_USER', 'DBT_PASSWORD',
        'TARGET_SCHEMA', 'TARGET_DB_SCHEMA'
    ]
    
    missing_vars = [var for var in required_vars if not env_vars.get(var)]
    if missing_vars:
        raise AirflowException(f"Missing required environment variables: {', '.join(missing_vars)}")
    LOGGER.info("✓ All required environment variables are set")
    
    LOGGER.info("All preflight checks passed successfully!")
    return True 