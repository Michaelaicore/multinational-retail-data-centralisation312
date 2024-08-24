"""
database_connector.py

This module provides functionality for connecting to a PostgreSQL database,
reading credentials, initializing a database engine, listing tables, and
uploading data to the database.

Classes:
--------
DatabaseConnector: Handles database connection and operations.

Methods:
--------
- read_db_creds: Reads database credentials from a YAML file.
- init_db_engine: Initializes an SQLAlchemy engine.
- list_db_tables: Lists all table names in the database.
- upload_to_db: Uploads a Pandas DataFrame to a specified table.
"""
import time
from typing import Dict

import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import psycopg2
import pandas as pd


class DatabaseConnector:
    """
    A class used to connect to a PostgreSQL database and perform operations.

    Methods
    -------
    read_db_creds(file_path='db_creds.yaml')
        Reads database credentials from a YAML file and returns them as a dictionary.

    init_db_engine()
        Initializes and returns an SQLAlchemy engine using the database credentials.

    list_db_tables()
        Lists all table names in the connected database.

    upload_to_db(df, table_name)
        Uploads a Pandas DataFrame to the specified table in the database.
    """

    def __init__(self):
        self.engine = None
        self.tables = []

    def read_db_creds(self, file_path: str = "db_creds.yaml") -> Dict[str, str]:
        """
        Reads the database credentials from a YAML file.

        Parameters
        ----------
        file_path : str
            The path to the YAML file containing database credentials.

        Returns
        -------
        dict
            A dictionary containing the database credentials.

        Raises
        ------
        FileNotFoundError
            If the YAML file is not found.

        yaml.YAMLError
            If the YAML file cannot be parsed.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                try:
                    creds = yaml.safe_load(file)
                except yaml.YAMLError as exc:
                    raise ValueError("Error parsing the YAML file.") from exc

            if not isinstance(creds, dict):
                raise ValueError("Invalid credentials format.")
            required_keys = {
                "RDS_USER",
                "RDS_PASSWORD",
                "RDS_HOST",
                "RDS_PORT",
                "RDS_DATABASE",
            }
            if not required_keys.issubset(creds.keys()):
                raise ValueError("Missing required database credentials.")

            return creds

        except FileNotFoundError as exc:
            raise FileNotFoundError("Credentials file not found.") from exc

        except yaml.YAMLError as exc:
            raise ValueError("Error parsing the YAML file.") from exc

    def init_db_engine(self) -> None:
        """
        Initializes and returns an SQLAlchemy engine for the database.

        Returns
        -------
        sqlalchemy.engine.base.Engine
            An SQLAlchemy engine connected to the specified database.

        Raises
        ------
        Exception
            If the engine cannot be created.
        """
        try:
            creds = self.read_db_creds()
            self.engine = create_engine(
                f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
        except KeyError as e:
            raise KeyError(f"Database credential error: {e}") from e
        except TypeError as e:
            raise TypeError(f"Database credential type error: {e}") from e
        except SQLAlchemyError as e:
            raise SQLAlchemyError(f"Failed to create database engine: {e}") from e

    def list_db_tables(self) -> None:
        """
        Lists all tables in the connected database.

        Returns
        -------
        list
            A list of table names in the database.

        Raises
        ------
        Exception
            If the tables cannot be listed.
        """
        try:
            inspector = inspect(self.engine)
            self.tables = inspector.get_table_names()
        except KeyError as e:
            raise KeyError(f"Failed to list database tables: {e}") from e
        except TypeError as e:
            raise TypeError(f"Failed to list database tables: {e}") from e

    def upload_to_db(self, df: pd.DataFrame, table_name: str) -> None:

        target_creds = self.read_db_creds('target_db_creds.yaml')
        self.target_engine = create_engine(
                f"postgresql://{target_creds['RDS_USER']}:{target_creds['RDS_PASSWORD']}@{target_creds['RDS_HOST']}:{target_creds['RDS_PORT']}/{target_creds['RDS_DATABASE']}")
        try:
            self.target_engine.connect()
            df.to_sql(table_name, self.target_engine, if_exists="replace", index=False)
        except SQLAlchemyError as e:
            # Raise an error with details
            raise SQLAlchemyError(f"Failed to upload data to the database: {e}") from e













