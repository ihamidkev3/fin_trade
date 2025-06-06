"""
Main entry point for the fin_trade ELT pipeline.
Can be run directly or called from Airflow DAG and GitHub Actions.
"""

from datetime import datetime
from .extract import extract_data
from .load import get_last_processed_data, load_to_sql_server
from ..config import DB_CONFIG
from ..utils.logger import LOGGER

def run_pipeline():
    """Run the ELT pipeline end-to-end."""
    try:
        # Get last processed data
        last_event_date, last_record_id = get_last_processed_data()
        
        # Extract and load
        df = extract_data(last_event_date, last_record_id)
        if not df.empty:
            load_to_sql_server(df)
            LOGGER.info(f"Processed {len(df)} records")
        else:
            LOGGER.info("No new data to process")
            
        return True
        
    except Exception as e:
        LOGGER.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline() 