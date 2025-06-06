"""
Logging setup utilities for the Financial Trading pipeline
"""
from airflow.exceptions import AirflowException
from scripts.utils.logger import LOGGER

def setup_logging(**context):
    """Log DAG execution information using existing logger."""
    try:
        dag_id = context['dag'].dag_id
        run_id = context['run_id']
        task_id = context['task'].task_id
        execution_date = context['execution_date']
        
        LOGGER.info(f"Starting DAG: {dag_id}")
        LOGGER.info(f"Run ID: {run_id}")
        LOGGER.info(f"Task ID: {task_id}")
        LOGGER.info(f"Execution Date: {execution_date}")
        LOGGER.info("Environment configuration:")
        for key, value in context['env_vars'].items():
            LOGGER.info(f"  {key}: {value}")
                
        return True
        
    except Exception as e:
        LOGGER.error(f"Failed to setup logging: {str(e)}")
        raise AirflowException(f"Logging setup failed: {str(e)}") 