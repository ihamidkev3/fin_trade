"""
Configuration package for fin_trade pipeline.
Contains database configurations.

Modules:
- database.py: Database connection and configuration settings
"""

from .database import DB_CONFIG

__all__ = ['DB_CONFIG'] 