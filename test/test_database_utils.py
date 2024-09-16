import unittest
from unittest.mock import patch, MagicMock
import yaml
import pandas as pd
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from main.database_utils import DatabaseConnector


import unittest
from unittest.mock import patch, MagicMock
import yaml
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from main.database_utils import DatabaseConnector


class TestDatabaseConnector(unittest.TestCase):

    @patch("main.database_utils.yaml.safe_load")
    @patch(
        "builtins.open",
        new_callable=unittest.mock.mock_open,
        read_data="RDS_USER: user\nRDS_PASSWORD: pass\nRDS_HOST: host\nRDS_PORT: 5432\nRDS_DATABASE: db",
    )
    def test_read_db_creds(self, mock_open, mock_yaml):
        mock_yaml.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "host",
            "RDS_PORT": 5432,
            "RDS_DATABASE": "db",
        }
        connector = DatabaseConnector()
        creds = connector.read_db_creds("dummy_path")
        expected_creds = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "host",
            "RDS_PORT": 5432,
            "RDS_DATABASE": "db",
        }
        self.assertEqual(creds, expected_creds)
        mock_open.assert_called_once_with("dummy_path", "r", encoding="utf-8")
        mock_yaml.assert_called_once()

    @patch("main.database_utils.create_engine")
    @patch("main.database_utils.DatabaseConnector.read_db_creds")
    def test_init_db_engine_success(self, mock_read_db_creds, mock_create_engine):
        mock_read_db_creds.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "host",
            "RDS_PORT": "5432",
            "RDS_DATABASE": "db",
        }
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        connector = DatabaseConnector()
        connector.init_db_engine()
        mock_create_engine.assert_called_once()
        mock_engine.connect.assert_called_once()

    @patch("main.database_utils.create_engine")
    @patch("main.database_utils.DatabaseConnector.read_db_creds")
    def test_init_db_engine_failure(self, mock_read_db_creds, mock_create_engine):
        mock_read_db_creds.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "host",
            "RDS_PORT": "5432",
            "RDS_DATABASE": "db",
        }
        mock_create_engine.side_effect = SQLAlchemyError("Connection error")
        connector = DatabaseConnector()
        with self.assertRaises(SQLAlchemyError):
            connector.init_db_engine()

    @patch("main.database_utils.inspect")
    @patch("main.database_utils.DatabaseConnector.get_engine")
    def test_list_db_tables(self, mock_get_engine, mock_inspect):
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = ["table1", "table2"]
        connector = DatabaseConnector()
        connector.init_db_engine()
        connector.list_db_tables()
        self.assertEqual(connector.tables, ["table1", "table2"])

    @patch("main.database_utils.create_engine")
    @patch("main.database_utils.DatabaseConnector.read_db_creds")
    def test_upload_to_db(self, mock_read_db_creds, mock_create_engine):
        mock_read_db_creds.return_value = {
            "RDS_USER": "user",
            "RDS_PASSWORD": "pass",
            "RDS_HOST": "host",
            "RDS_PORT": "5432",
            "RDS_DATABASE": "db",
        }
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        connector = DatabaseConnector(target_creds_path="target_db_creds.yaml")
        connector.upload_to_db(df, "table_name")
        mock_create_engine.assert_called_once()
        mock_engine.connect.assert_called_once()
        mock_engine.dispose.assert_called_once()

    @patch("main.database_utils.create_engine")
    def test_close_connections(self, mock_create_engine):
        mock_engine = MagicMock()
        connector = DatabaseConnector()
        connector.engine = mock_engine
        connector.target_engine = mock_engine
        connector.close_connections()
        mock_engine.dispose.assert_called()
        self.assertIsNone(connector.engine)
        self.assertIsNone(connector.target_engine)


if __name__ == "__main__":
    unittest.main()


# class TestDatabaseConnector(unittest.TestCase):

#     @patch('DatabaseConnector.yaml.safe_load')
#     @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='RDS_USER: user\nRDS_PASSWORD: pass\nRDS_HOST: host\nRDS_PORT: 5432\nRDS_DATABASE: db')
#     def test_read_db_creds(self, mock_open, mock_yaml):
#         connector = DatabaseConnector()
#         creds = connector.read_db_creds('dummy_path')
#         expected_creds = {
#             'RDS_USER': 'user',
#             'RDS_PASSWORD': 'pass',
#             'RDS_HOST': 'host',
#             'RDS_PORT': '5432',
#             'RDS_DATABASE': 'db'
#         }
#         self.assertEqual(creds, expected_creds)
#         mock_open.assert_called_once_with('dummy_path', 'r', encoding='utf-8')
#         mock_yaml.assert_called_once()

#     @patch('DatabaseConnector.create_engine')
#     @patch('DatabaseConnector.read_db_creds')
#     def test_init_db_engine_success(self, mock_read_db_creds, mock_create_engine):
#         mock_read_db_creds.return_value = {
#             'RDS_USER': 'user',
#             'RDS_PASSWORD': 'pass',
#             'RDS_HOST': 'host',
#             'RDS_PORT': '5432',
#             'RDS_DATABASE': 'db'
#         }
#         mock_engine = MagicMock()
#         mock_create_engine.return_value = mock_engine
#         connector = DatabaseConnector()
#         connector.init_db_engine()
#         mock_create_engine.assert_called_once()
#         mock_engine.connect.assert_called_once()

#     @patch('DatabaseConnector.create_engine')
#     @patch('DatabaseConnector.DatabaseConnector.read_db_creds')
#     def test_init_db_engine_failure(self, mock_read_db_creds, mock_create_engine):
#         mock_read_db_creds.return_value = {
#             'RDS_USER': 'user',
#             'RDS_PASSWORD': 'pass',
#             'RDS_HOST': 'host',
#             'RDS_PORT': '5432',
#             'RDS_DATABASE': 'db'
#         }
#         mock_create_engine.side_effect = SQLAlchemyError("Connection error")
#         connector = DatabaseConnector()
#         with self.assertRaises(SQLAlchemyError):
#             connector.init_db_engine()

#     @patch('DatabaseConnector.inspect')
#     @patch('DatabaseConnector.DatabaseConnector.get_engine')
#     def test_list_db_tables(self, mock_get_engine, mock_inspect):
#         mock_engine = MagicMock()
#         mock_get_engine.return_value = mock_engine
#         mock_inspector = MagicMock()
#         mock_inspect.return_value = mock_inspector
#         mock_inspector.get_table_names.return_value = ['table1', 'table2']
#         connector = DatabaseConnector()
#         connector.init_db_engine()
#         connector.list_db_tables()
#         self.assertEqual(connector.tables, ['table1', 'table2'])

#     @patch('DatabaseConnector.create_engine')
#     @patch('DatabaseConnector.DatabaseConnector.read_db_creds')
#     def test_upload_to_db(self, mock_read_db_creds, mock_create_engine):
#         mock_read_db_creds.return_value = {
#             'RDS_USER': 'user',
#             'RDS_PASSWORD': 'pass',
#             'RDS_HOST': 'host',
#             'RDS_PORT': '5432',
#             'RDS_DATABASE': 'db'
#         }
#         mock_engine = MagicMock()
#         mock_create_engine.return_value = mock_engine
#         df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
#         connector = DatabaseConnector(target_creds_path='target_db_creds.yaml')
#         connector.upload_to_db(df, 'table_name')
#         mock_create_engine.assert_called_once()
#         mock_engine.connect.assert_called_once()
#         mock_engine.dispose.assert_called_once()

#     @patch('DatabaseConnector.create_engine')
#     def test_close_connections(self, mock_create_engine):
#         mock_engine = MagicMock()
#         connector = DatabaseConnector()
#         connector.engine = mock_engine
#         connector.target_engine = mock_engine
#         connector.close_connections()
#         mock_engine.dispose.assert_called()
#         self.assertIsNone(connector.engine)
#         self.assertIsNone(connector.target_engine)

# if __name__ == '__main__':
#     unittest.main()
