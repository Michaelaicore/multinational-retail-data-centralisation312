import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from typing import Dict
from main.database_utils import (
    DatabaseConnector,
)  # Replace with your actual module name


class TestDatabaseConnector(unittest.TestCase):
    def setUp(self):
        self.connector = DatabaseConnector()

    # @patch("builtins.open", new_callable=mock_open, read_data="RDS_USER: user\nRDS_PASSWORD: pass\nRDS_HOST: localhost\nRDS_PORT: 5432\nRDS_DATABASE: test_db")
    # @patch("yaml.safe_load")
    # def test_read_db_creds(self, mock_safe_load, mock_open_file):
    #     # Mock the yaml.safe_load to return expected credentials
    #     mock_safe_load.return_value = {
    #         "RDS_USER": "user",
    #         "RDS_PASSWORD": "pass",
    #         "RDS_HOST": "localhost",
    #         "RDS_PORT": "5432",
    #         "RDS_DATABASE": "test_db"
    #     }

    #     creds = self.connector.read_db_creds()

    #     self.assertEqual(creds["RDS_USER"], "user")
    #     self.assertEqual(creds["RDS_PASSWORD"], "pass")
    #     self.assertEqual(creds["RDS_HOST"], "localhost")
    #     self.assertEqual(creds["RDS_PORT"], "5432")
    #     self.assertEqual(creds["RDS_DATABASE"], "test_db")
    #     mock_open_file.assert_called_once_with("db_creds.yaml", 'r', encoding='utf-8')
    #     mock_safe_load.assert_called_once()

    @patch("main.database_utils.yaml.safe_load")
    @patch("builtins.open")
    def test_read_db_creds(self, mock_open, mock_yaml):
        mock_yaml.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "password",
            "RDS_HOST": "host",
            "RDS_PORT": "5432",
            "RDS_DATABASE": "db",
        }
        creds = self.connector.read_db_creds()
        self.assertEqual(creds["RDS_USER"], "user")

    @patch("builtins.open", new_callable=mock_open)
    def test_read_db_creds_file_not_found(self, mock_open_file):
        mock_open_file.side_effect = FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            self.connector.read_db_creds()

    @patch("main.database_utils.yaml.safe_load")
    def test_read_db_creds_yaml_error(self, mock_safe_load):
        mock_safe_load.side_effect = yaml.YAMLError
        with self.assertRaises(ValueError):
            self.connector.read_db_creds()

    @patch("main.database_utils.create_engine")
    @patch.object(DatabaseConnector, "read_db_creds")
    def test_init_db_engine(self, mock_read_db_creds, mock_create_engine):
        mock_read_db_creds.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "localhost",
            "RDS_PORT": "5432",
            "RDS_DATABASE": "test_db",
        }

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        self.connector.init_db_engine()

        mock_create_engine.assert_called_once_with(
            "postgresql://user:pass@localhost:5432/test_db"
        )
        self.assertEqual(self.connector.engine, mock_engine)

    @patch("main.database_utils.create_engine")
    @patch.object(DatabaseConnector, "read_db_creds")
    def test_init_db_engine_sqlalchemy_error(
        self, mock_read_db_creds, mock_create_engine
    ):
        mock_read_db_creds.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "localhost",
            "RDS_PORT": "5432",
            "RDS_DATABASE": "test_db",
        }
        mock_create_engine.side_effect = SQLAlchemyError

        with self.assertRaises(SQLAlchemyError):
            self.connector.init_db_engine()

    @patch("main.database_utils.inspect")
    @patch.object(DatabaseConnector, "init_db_engine")
    def test_list_db_tables(self, mock_init_db_engine, mock_inspect):
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = ["table1", "table2"]

        self.connector.engine = MagicMock()
        self.connector.list_db_tables()

        mock_inspect.assert_called_once_with(self.connector.engine)
        self.assertEqual(self.connector.tables, ["table1", "table2"])

    @patch("main.database_utils.inspect")
    def test_list_db_tables_error(self, mock_inspect):
        mock_inspect.side_effect = SQLAlchemyError
        self.connector.engine = MagicMock()

        with self.assertRaises(SQLAlchemyError):
            self.connector.list_db_tables()

    @patch("main.database_utils.pd.DataFrame.to_sql")
    @patch("main.database_utils.create_engine")
    def test_upload_to_db(self, mock_create_engine, mock_to_sql):
        mock_engine = mock_create_engine.return_value
        mock_connection = mock_engine.connect.return_value.__enter__.return_value

        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        table_name = "test_table"

        # Act
        self.connector.engine = mock_engine
        self.connector.upload_to_db(df, table_name)

        # Assert
        mock_to_sql.assert_called_once_with(
            table_name, con=mock_connection, if_exists="replace", index=False
        )

    def test_upload_to_db_empty_dataframe(self):
        df = pd.DataFrame()
        with self.assertRaises(ValueError):
            self.connector.upload_to_db(df, "test_table")

    def test_upload_to_db_invalid_table_name(self):
        df = pd.DataFrame({"column1": [1, 2, 3]})
        with self.assertRaises(ValueError):
            self.connector.upload_to_db(df, 12345)

    @patch("pandas.DataFrame.to_sql")
    def test_upload_to_db_sqlalchemy_error(self, mock_to_sql):
        mock_to_sql.side_effect = SQLAlchemyError
        df = pd.DataFrame({"column1": [1, 2, 3]})
        self.connector.engine = MagicMock()

        with self.assertRaises(SQLAlchemyError):
            self.connector.upload_to_db(df, "test_table")


if __name__ == "__main__":
    unittest.main()
