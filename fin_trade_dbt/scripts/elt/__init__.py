"""
ELT package for fin_trade pipeline.
Contains extract, load, and pipeline operations.

Modules:
- extract.py: Handles data extraction from MySQL source database
- load.py: Manages data loading to SQL Server target database
- pipeline.py: Main entry point for running the ELT pipeline

Usage:
    To run the pipeline directly:
    $ python -m fin_trade.scripts.elt.pipeline

    The pipeline can also be executed from Airflow DAGs or GitHub Actions
    by importing and running the pipeline.py module.
"""

from .extract import extract_data
from .load import load_to_sql_server, get_last_processed_data
from .pipeline import run_el_pipeline

__all__ = ['extract_data', 'load_to_sql_server', 'get_last_processed_data', 'run_el_pipeline'] 