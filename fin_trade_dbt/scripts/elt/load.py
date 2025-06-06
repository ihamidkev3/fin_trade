"""
Data loading module for fin_trade pipeline.
Handles data loading to SQL Server target database.
"""

import sqlalchemy
import pandas as pd
from ..config import DB_CONFIG
from ..utils.logger import LOGGER

def get_sql_server_engine(config):
    """
    Create SQL Server SQLAlchemy engine for database connection.
    
    Args:
        config (dict): Dictionary containing SQL Server connection parameters
                      (server, username, password, driver)
    
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance for SQL Server connection
    
    Raises:
        sqlalchemy.exc.SQLAlchemyError: If connection creation fails
    """
    conn_str = (
        f"mssql+pyodbc://{config['username']}:{config['password']}"
        f"@{config['server']}:{config['port']}/{config['database']}"
        f"?driver={config['driver']}"
        f"&encrypt={config['encrypt']}"
        f"&TrustServerCertificate={config['trust_server_certificate']}"
    )
    
    return sqlalchemy.create_engine(
        conn_str,
        pool_size=config['pool_size'],
        connect_args={
            'timeout': config['timeout'],
            'application_name': config['application_name']
        }
    )

def get_last_processed_data():
    """
    Get the last processed record's timestamp and ID from the fact table.
    
    Returns:
        tuple: (last_timestamp, last_record_id)
               - last_timestamp (str): ISO format timestamp of last processed record
               - last_record_id (int): ID of last processed record
    
    Note:
        Returns default values ("2000-01-01T00:00:00", 0) if no records found
        or in case of error
    """
    try:
        config = DB_CONFIG['SQLSERVER_CONFIG']
        engine = get_sql_server_engine(config)
        query = """
        SELECT MAX(creation_timestamp) as last_ts, 
               MAX(record_id) as last_id 
        FROM fact_financial_events
        """
        
        with engine.connect() as connection:
            result = pd.read_sql(sqlalchemy.text(query), connection)
            
        last_ts = result['last_ts'].iloc[0]
        last_id = result['last_id'].iloc[0]
        
        if pd.isna(last_ts):
            last_ts = "2000-01-01T00:00:00"
        if pd.isna(last_id):
            last_id = 0
            
        LOGGER.info(f"Last processed record: timestamp = {last_ts}, id = {last_id}")
        return last_ts, last_id
        
    except Exception as e:
        LOGGER.error(f"Error getting last processed data: {e}")
        return "2000-01-01T00:00:00", 0

def load_to_sql_server(df):
    """
    Load DataFrame to SQL Server staging table.
    
    Args:
        df (pandas.DataFrame): Data to be loaded
    
    Raises:
        Exception: If data loading fails
    
    Note:
        - Skips operation if DataFrame is empty
        - Uses batch loading with chunk size of 1000
        - Appends data to existing table
    """
    if df.empty:
        LOGGER.info("No data to load.")
        return
        
    config = DB_CONFIG['SQLSERVER_CONFIG']
    dbt_config = DB_CONFIG['DBT_CONFIG']
    
    LOGGER.info(f"Loading {len(df)} rows into SQL Server table: {dbt_config['target_schema']}.staging_financial_events")
    engine = get_sql_server_engine(config)
    
    try:
        df.to_sql(
            name='staging_financial_orders',
            con=engine,
            schema=dbt_config['target_schema'],
            if_exists='append',
            index=False,
            chunksize=1000
        )
        LOGGER.info(f"Successfully loaded data into {dbt_config['target_schema']}.staging_financial_events.")
        
    except Exception as e:
        LOGGER.error(f"Error loading data to SQL Server: {e}")
        raise 