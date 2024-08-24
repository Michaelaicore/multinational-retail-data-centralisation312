import unittest
from unittest.mock import patch, MagicMock
from main.data_extraction import DataExtractor
import pandas as pd


class TestDataExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = DataExtractor()

    @patch("data_extractor.DataExtractor.read_db_creds")
    def test_init_db_engine(self, mock_read_db_creds):
        # Mock database credentials
        mock_read_db_creds.return_value = {
            "user": "test_user",
            "password": "test_password",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
        }
        engine = self.extractor.init_db_engine()
        self.assertIsNotNone(engine)

    @patch("data_extractor.DataExtractor.list_db_tables")
    def test_list_db_tables(self, mock_list_db_tables):
        mock_list_db_tables.return_value = ["users", "orders"]
        tables = self.extractor.list_db_tables()
        self.assertIn("users", tables)
        self.assertIn("orders", tables)

    @patch("data_extractor.DataExtractor.list_db_tables")
    @patch("pandas.read_sql")
    def test_read_rds_table(self, mock_read_sql, mock_list_db_tables):
        mock_list_db_tables.return_value = ["users"]
        mock_read_sql.return_value = pd.DataFrame({"id": [1], "name": ["Alice"]})

        df = self.extractor.read_rds_table("users")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertIn("name", df.columns)

    @patch("data_extractor.DataExtractor.list_db_tables")
    def test_read_rds_table_nonexistent_table(self, mock_list_db_tables):
        mock_list_db_tables.return_value = ["users"]

        with self.assertRaises(ValueError):
            self.extractor.read_rds_table("nonexistent")


if __name__ == "__main__":
    unittest.main()
