"""
Data extraction module for fin_trade pipeline.
Handles data extraction from MySQL source database.
"""

import sqlalchemy
import pandas as pd
from ..config import DB_CONFIG
from ..utils.logger import LOGGER

def get_mysql_engine(config):
    """
    Create MySQL SQLAlchemy engine for database connection.
    
    Args:
        config (dict): Dictionary containing MySQL connection parameters
                      (server, port, database, username, password)
    
    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance for MySQL connection
    
    Raises:
        sqlalchemy.exc.SQLAlchemyError: If connection creation fails
    """
    connection_url = (
        f"mysql+pymysql://{config['username']}:{config['password']}"
        f"@{config['server']}:{config['port']}/{config['database']}"
        f"?charset={config['charset']}"
        f"&ssl_verify_cert={config['ssl_verify_cert']}"
        f"&ssl_verify_identity={config['ssl_verify_identity']}"
    )
    return sqlalchemy.create_engine(
        connection_url,
        pool_size=config['pool_size'],
        pool_name=config['pool_name'],
        connect_args={
            'connect_timeout': config['connection_timeout'],
            'read_timeout': config['read_timeout'],
            'write_timeout': config['write_timeout']
        }
    )

def extract_data(last_event_date="2000-01-01T00:00:00", last_record_id=0):
    """
    Extract data from MySQL source database with incremental loading support.
    
    Args:
        last_event_date (str): Timestamp of the last processed record (ISO format)
        last_record_id (int): ID of the last processed record
    
    Returns:
        pandas.DataFrame: DataFrame containing extracted data
    
    Raises:
        Exception: If data extraction fails
    """
    mysql_config = DB_CONFIG['MYSQL_CONFIG']

    LOGGER.info(f"Fetching data from {mysql_config['database']} "
                f"since ts > '{last_event_date}' AND id > {last_record_id}")
    
    query_sql = """
    SELECT 
        -- Primary identifier for the record
        record_id,
        -- Timestamps for record lifecycle
        creation_timestamp,
        modification_timestamp,
        deletion_timestamp,
        -- User information
        user_identifier,
        user_email,
        -- order status information
        status_code,
        -- Asset information
        quote_currency,
        base_currency,
        quote_amount,
        base_amount,
        -- Price and order details
        execution_price,
        order_direction,
        market_symbol,
        order_type,
        -- Execution details
        executed_amount,
        total_quote_executed,
        time_validity,
        fee_amount,
        -- Provider details
        provider_quoted_price,
        provider_fee,
        liquidity_provider,
        provider_response
    FROM financial_events
    WHERE (created_at > :last_create_date OR (created_at = :last_create_date AND id > :last_natural_key))
    AND created_at < CURDATE()
    ORDER BY created_at, id;
    """
    
    try:
        engine = get_mysql_engine(mysql_config)
        with engine.connect() as connection:
            df = pd.read_sql(sqlalchemy.text(query_sql), connection, params={
                "last_create_date": last_event_date,
                "last_natural_key": last_record_id
            })
        LOGGER.info(f"Fetched {len(df)} new rows from financial_events table.")
        return df
    except Exception as e:
        LOGGER.error(f"Error extracting data from MySQL: {e}")
        raise 