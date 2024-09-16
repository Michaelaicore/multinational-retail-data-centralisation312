import unittest
import json
from unittest.mock import patch, MagicMock
import pandas as pd
from io import StringIO
from main.data_extraction import DataExtractor
from main.database_utils import DatabaseConnector


class TestDataExtractor(unittest.TestCase):

    @patch("main.data_extraction.boto3.client")
    def setUp(self, mock_boto_client):
        # Initialize the DataExtractor object with mocks
        self.extractor = DataExtractor()

    @patch("main.data_extraction.pd.read_sql")
    def test_read_rds_table(self, mock_read_sql):
        # Mocking the database query result
        mock_df = pd.DataFrame({"column1": [1, 2, 3]})
        mock_read_sql.return_value = mock_df

        result_df = self.extractor.read_rds_table("test_table")
        self.assertEqual(result_df.equals(mock_df), True)
        mock_read_sql.assert_called_once()

    @patch("main.data_extraction.jpype.startJVM")
    @patch("main.data_extraction.tabula.read_pdf")
    def test_retrieve_pdf_data(self, mock_read_pdf, mock_start_jvm):
        # Mocking the PDF data extraction
        mock_df = pd.DataFrame({"column1": [1, 2, 3]})
        mock_read_pdf.return_value = [mock_df]

        result_df = self.extractor.retrieve_pdf_data("test_link")
        self.assertEqual(result_df.equals(mock_df), True)
        mock_read_pdf.assert_called_once()

    @patch("main.data_extraction.requests.get")
    def test_list_number_of_stores(self, mock_get):
        # Mocking the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"number_stores": 10}
        mock_get.return_value = mock_response

        num_stores = self.extractor.list_number_of_stores(
            "test_endpoint", {"header": "value"}
        )
        self.assertEqual(num_stores, 10)
        mock_get.assert_called_once()

    @patch("main.data_extraction.requests.get")
    def test_retrieve_stores_data(self, mock_get):
        # Mocking the API responses for two stores with flat structures
        mock_responses = [
            {"index": 1, "country_code": "GB", "continent": "Europe"},
            {"index": 2, "country_code": "DE", "continent": "Europe"},
        ]

        # Mocking requests.get to return different responses for each API call
        mock_get.side_effect = [
            MagicMock(
                json=lambda: mock_responses[0]
            ),  # First API call returns the first store
            MagicMock(
                json=lambda: mock_responses[1]
            ),  # Second API call returns the second store
        ]

        # Call the method being tested
        result_df = self.extractor.retrieve_stores_data(
            "test_endpoint/{store_number}", {"header": "value"}, 2
        )

        # Expected DataFrame constructed from the mock response
        expected_df = pd.DataFrame(mock_responses)

        # Check if result_df matches expected_df
        print("Expected DataFrame:\n", expected_df)
        print("Result DataFrame:\n", result_df)

        # Assert that the two DataFrames are equal
        self.assertTrue(result_df.equals(expected_df))
        # Ensure requests.get was called twice
        self.assertEqual(mock_get.call_count, 2)

    @patch("main.data_extraction.requests.get")
    def test_extract_from_s3(self, mock_s3_client):
        # Create a mock S3 client
        mock_s3 = mock_s3_client.return_value

        # Mock the get_object method to return bytes data
        mock_s3.get_object.return_value = {"Body": MagicMock()}
        mock_s3.get_object.return_value["Body"].read.return_value = (
            b"col1,col2\nval1,val2\nval3,val4\n"
        )
        # Call the method being tested
        result_df = self.extractor.extract_from_s3("s3://bucket_name/path/to/file.csv")

        # Expected DataFrame
        expected_df = pd.DataFrame({"col1": ["val1", "val3"], "col2": ["val2", "val4"]})

        # Assert that the DataFrame matches the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("boto3.client")
    def test_extract_from_s3(self, mock_s3_client):

        # Create a mock S3 client
        mock_s3 = mock_s3_client.return_value

        # Mock the get_object method to return bytes data
        mock_s3_object = MagicMock()
        mock_s3_object.read.return_value = b"col1,col2\nval1,val2\nval3,val4\n"

        mock_s3_client.get_object.return_value = {"Body": mock_s3_object}

        # Initialize DataExtractor

        self.extractor.s3 = mock_s3_client

        mock_s3.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(return_value=b"col1,col2\nval1,val2\nval3,val4\n")
            )
        }

        # Call the method being tested
        result_df = self.extractor.extract_from_s3("s3://bucket_name/path/to/file.csv")

        # Expected DataFrame
        expected_df = pd.DataFrame({"col1": ["val1", "val3"], "col2": ["val2", "val4"]})

        # Assert that the DataFrame matches the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch("main.data_extraction.DataCleaning.validate_and_clean_data")
    @patch("main.data_extraction.DataCleaning.get_valid_data")
    @patch("main.data_extraction.DataCleaning.get_invalid_data")
    @patch("main.data_extraction.DataCleaning.save_invalid_data_log")
    def test_process_data(
        self,
        mock_save_invalid_data_log,
        mock_get_invalid_data,
        mock_get_valid_data,
        mock_validate_and_clean_data,
    ):
        # Mocking data cleaning and validation process
        mock_valid_df = pd.DataFrame({"column1": [1, 2]})
        mock_invalid_df = pd.DataFrame({"column1": [3]})
        mock_get_valid_data.return_value = mock_valid_df
        mock_get_invalid_data.return_value = mock_invalid_df

        self.extractor.process_data()

        self.assertEqual(self.extractor.valid_df.equals(mock_valid_df), True)
        mock_validate_and_clean_data.assert_called_once()
        mock_get_valid_data.assert_called_once()
        mock_get_invalid_data.assert_called_once()
        mock_save_invalid_data_log.assert_called_once()


if __name__ == "__main__":
    unittest.main()
