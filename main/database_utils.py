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
import logging
from typing import Dict
import yaml
import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import pandas as pd


class DatabaseConnector:
    """
    A class used to connect to a PostgreSQL database and perform operations.

    Attributes
    ----------
    creds_path : str
        The path to the YAML file containing database credentials.
    target_creds_path : str
        The path to the YAML file containing target database credentials.
    engine : sqlalchemy.engine.base.Engine
        The SQLAlchemy engine for the source database.
    target_engine : sqlalchemy.engine.base.Engine
        The SQLAlchemy engine for the target database.
    tables : list
        List of table names in the connected database.
    logger : logging.Logger
        Logger for recording operational messages.

    Methods
    -------
    read_db_creds(file_path: str) -> Dict[str, str]
        Reads database credentials from a YAML file.
    init_db_engine(retries: int = 3, delay: int = 5) -> None
        Initializes and connects to the source database engine.
    list_db_tables() -> None
        Lists all tables in the source database.
    upload_to_db(df: pd.DataFrame, table_name: str, retries: int = 3, delay: int = 5) -> None
        Uploads a DataFrame to the target database table.
    get_engine() -> sqlalchemy.engine.base.Engine
        Returns the SQLAlchemy engine for the source database.
    close_connections() -> None
        Closes the connections to the databases.
    """

    def __init__(
        self,
        creds_path: str = "db_creds.yaml",
        target_creds_path: str = "target_db_creds.yaml",
    ):
        """
        Initializes the DatabaseConnector with the paths to the credentials files.

        Parameters
        ----------
        creds_path : str
            The path to the YAML file containing database credentials.
        target_creds_path : str
            The path to the YAML file containing target database credentials.
        """
        self.creds_path = creds_path
        self.target_creds_path = target_creds_path
        self.engine = None
        self.target_engine = None
        self.tables = []

        # Setup logger
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def read_db_creds(self, file_path: str) -> Dict[str, str]:
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
        ValueError
            If the YAML file content is invalid.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                creds = yaml.safe_load(file)
                if not isinstance(creds, dict):
                    raise ValueError("Invalid YAML format.")

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
        except FileNotFoundError:
            self.logger.error("Credentials file not found.")
            raise
        except yaml.YAMLError:
            self.logger.error("Error parsing the YAML file.")
            raise
        except ValueError as e:
            self.logger.error(str(e))
            raise

    def init_db_engine(self, retries: int = 3, delay: int = 5) -> None:
        """
        Initializes and connects to the source database engine, with retry logic.

        Parameters
        ----------
        retries : int
            Number of times to retry the connection in case of failure.
        delay : int
            Delay between retries in seconds.

        Raises
        ------
        SQLAlchemyError
            If the engine cannot be created after retries.
        """
        for attempt in range(retries):
            try:
                creds = self.read_db_creds(self.creds_path)
                self.engine = create_engine(
                    f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
                )
                self.engine.connect()
                self.logger.info("Database engine created successfully.")
                break
            except (OperationalError, SQLAlchemyError) as e:
                self.logger.error(f"Failed to create database engine: {e}")
                if attempt < retries - 1:
                    self.logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise SQLAlchemyError(
                        f"Failed to create database engine after {retries} attempts."
                    ) from e

    def list_db_tables(self) -> None:
        """
        Lists all tables in the connected database.

        Raises
        ------
        ValueError
            If the database engine is not initialized.
        SQLAlchemyError
            If the tables cannot be listed.
        """
        if not self.engine:
            self.logger.error("Database engine is not initialized.")
            raise ValueError("Database engine is not initialized.")

        try:
            inspector = inspect(self.engine)
            self.tables = inspector.get_table_names()
            self.logger.info(f"Tables in the database: {self.tables}")
        except SQLAlchemyError as e:
            self.logger.error(f"Failed to list database tables: {e}")
            raise

    def upload_to_db(
        self, df: pd.DataFrame, table_name: str, retries: int = 3, delay: int = 5
    ) -> None:
        """
        Uploads a DataFrame to the target database table, with retry logic.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing the data to upload.
        table_name : str
            The name of the table to upload the data to.
        retries : int
            Number of times to retry the upload in case of failure.
        delay : int
            Delay between retries in seconds.

        Raises
        ------
        SQLAlchemyError
            If the upload fails after retries.
        """
        target_creds = self.read_db_creds(self.target_creds_path)

        for attempt in range(retries):
            try:
                if not self.target_engine:
                    self.target_engine = create_engine(
                        f"postgresql://{target_creds['RDS_USER']}:{target_creds['RDS_PASSWORD']}@{target_creds['RDS_HOST']}:{target_creds['RDS_PORT']}/{target_creds['RDS_DATABASE']}"
                    )
                    self.target_engine.connect()

                df.to_sql(
                    table_name, self.target_engine, if_exists="replace", index=False
                )
                self.logger.info(f"Data uploaded to table '{table_name}' successfully.")
                break
            except SQLAlchemyError as e:
                self.logger.error(f"Failed to upload data to the database: {e}")
                if attempt < retries - 1:
                    self.logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise SQLAlchemyError(
                        f"Failed to upload data after {retries} attempts."
                    ) from e
            finally:
                self.close_connections()

    def get_engine(self):
        """
        Returns the SQLAlchemy engine for the source database.

        Returns
        -------
        sqlalchemy.engine.base.Engine
            The SQLAlchemy engine.
        """
        return self.engine

    def close_connections(self) -> None:
        """
        Closes the connections to the databases.
        """
        if self.engine:
            self.engine.dispose()
            self.engine = None  # Ensure it's set to None after disposal
        if self.target_engine:
            self.target_engine.dispose()
            self.target_engine = None  # Ensure it's set to None after disposal
        self.logger.info("Source database connection closed.")
        self.logger.info("Target database connection closed.")


if __name__ == "__main__":
    # Example usage
    db_connector = DatabaseConnector()
    db_connector.init_db_engine()
    db_connector.list_db_tables()
    # Assuming df is a DataFrame to be uploaded

    print(db_connector.tables)
