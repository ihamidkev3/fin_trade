"""
Database configuration settings for fin_trade pipeline.
This module contains database connection configurations for both source (MySQL) and target (SQL Server) databases,
as well as dbt-specific configurations for data transformations.
"""

import os

DB_CONFIG = {
    # dbt Configuration
    # These settings are used by dbt for managing schemas and data transformations
    'DBT_CONFIG': {
        'target_schema': os.environ.get('TARGET_SCHEMA'),  # Schema where dbt will create transformed tables
        'target_db': os.environ.get('TARGET_DB_SCHEMA'),  # Database where dbt will operate
        'models_path': os.environ.get('DBT_MODELS_PATH', 'models'),  # Path to dbt models
        'profiles_dir': os.environ.get('DBT_PROFILES_DIR', './'),  # dbt profiles directory
        'target': os.environ.get('DBT_TARGET', 'dev')  # dbt target environment (dev/prod)
    },
    
    # Source Database (MySQL)
    # Configuration for the source MySQL database containing raw financial data
    'MYSQL_CONFIG': {
        'server': os.environ.get('MYSQL_SERVER'),
        'port': int(os.environ.get('MYSQL_PORT')),
        'username': os.environ.get('MYSQL_USER'),
        'password': os.environ.get('MYSQL_PASSWORD'),
        'database': os.environ.get('MYSQL_DATABASE'),
        # SSL/TLS Security settings
        'ssl_verify_cert': os.environ.get('MYSQL_SSL_VERIFY_CERT', 'false'),
        'ssl_verify_identity': os.environ.get('MYSQL_SSL_VERIFY_IDENTITY', 'false'),
        # Timeout configurations
        'connection_timeout': int(os.environ.get('MYSQL_CONNECTION_TIMEOUT', '300')),
        'read_timeout': int(os.environ.get('MYSQL_READ_TIMEOUT', '300')),
        'write_timeout': int(os.environ.get('MYSQL_WRITE_TIMEOUT', '300')),
        # Additional MySQL specific settings
        'charset': os.environ.get('MYSQL_CHARSET', 'utf8mb4'),
        'collation': os.environ.get('MYSQL_COLLATION', 'utf8mb4_general_ci'),
        'autocommit': os.environ.get('MYSQL_AUTOCOMMIT', 'false').lower() == 'true',
        'raise_on_warnings': os.environ.get('MYSQL_RAISE_WARNINGS', 'true').lower() == 'true',
        'get_warnings': True,
        'pool_size': int(os.environ.get('MYSQL_POOL_SIZE', '5')),
        'pool_name': os.environ.get('MYSQL_POOL_NAME', 'fin_trade_pool')
    },
    
    # Target Database (SQL Server)
    # Configuration for the target SQL Server database where transformed data will be loaded
    'SQLSERVER_CONFIG': {
        'server': os.environ.get('SQL_SERVER_IP'),
        'port': os.environ.get('SQL_SERVER_PORT'),
        'database': os.environ.get('TARGET_DB_SCHEMA'),
        'username': os.environ.get('DBT_USER'),
        'password': os.environ.get('DBT_PASSWORD'),
        'driver': os.environ.get('SQL_SERVER_DRIVER', 'ODBC Driver 17 for SQL Server'),
        # Security settings
        'encrypt': os.environ.get('SQL_SERVER_ENCRYPT', 'false'),
        'trust_server_certificate': os.environ.get('SQL_SERVER_TRUST_CERT', 'true'),
        # Performance and reliability settings
        'command_timeout': int(os.environ.get('SQL_COMMAND_TIMEOUT', '300')),
        'retries': int(os.environ.get('SQL_RETRIES', '3')),
        'pool_size': int(os.environ.get('SQL_POOL_SIZE', '5')),
        # Additional SQL Server specific settings
        'application_name': os.environ.get('SQL_APPLICATION_NAME', 'fin_trade_etl'),
        'schema': os.environ.get('SQL_DEFAULT_SCHEMA', 'dbo'),
        'timeout': int(os.environ.get('SQL_TIMEOUT', '300'))
    }

} 