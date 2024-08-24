import unittest
import pandas as pd
import numpy as np
import uuid
from main.data_cleaning import DataCleaning


class TestDataCleaning(unittest.TestCase):
    def setUp(self):
        """
        Set up a sample DataFrame and DataCleaning object for testing.
        """
        data = {
            "index": ["1", "-1", "abc", "3"],
            "first_name": ["John", "Jane", "Klaus-D.", ""],
            "last_name": ["Doe", "Smith", "O'Reilly", "123"],
            "date_of_birth": ["1990-01-01", "1995-07-07", "invalid date", np.nan],
            "company": ["OpenAI", "  ", "Microsoft", None],
            "email_address": [
                "john.doe@example.com",
                "jane@@example.com",
                "bad email",
                "",
            ],
            "address": ["123 Main St.\nApt 4B", "456 Oak Ave", "", None],
            "country": ["United States", "Germany", "InvalidCountry", ""],
            "country_code": ["US", "DE", "GGB", "invalid"],
            "phone_number": ["+49(0)123456789", "0044123456789", "(0)789456", ""],
            "join_date": ["2022-01-01", "2022-01-15", "not a date", np.nan],
            "user_uuid": [str(uuid.uuid4()), str(uuid.uuid4()), "invalid_uuid", ""],
        }
        self.df = pd.DataFrame(data)
        self.country_code = {
            "Germany": "DE",
            "United Kingdom": "GB",
            "United States": "US",
        }
        self.cleaner = DataCleaning(self.df, self.country_code)

    def test_validate_index(self):
        # Apply the validation
        self.cleaner.validate_index("index")

        # Get the result from the DataFrame after cleaning
        result = self.cleaner.df["index"].tolist()

        # Expected result
        expected = [1.0, np.nan, np.nan, 3.0]

        # Check if lists are equal considering NaN values
        for res, exp in zip(result, expected):
            if pd.isna(res) and pd.isna(exp):
                continue
            self.assertEqual(res, exp)

    def test_validate_name(self):
        """
        Test the validate_name method.
        """
        self.cleaner.validate_name("first_name")
        self.cleaner.validate_name("last_name")
        expected_first_names = ["John", "Jane", "Klaus-D.", np.nan]
        expected_last_names = ["Doe", "Smith", "O'Reilly", np.nan]
        self.assertEqual(self.cleaner.df["first_name"].tolist(), expected_first_names)
        self.assertEqual(self.cleaner.df["last_name"].tolist(), expected_last_names)

    def test_validate_date(self):
        """
        Test the validate_date method.
        """
        self.cleaner.validate_date("date_of_birth")
        self.cleaner.validate_date("join_date")
        self.assertTrue(pd.notna(self.cleaner.df["date_of_birth"][0]))
        self.assertTrue(pd.isna(self.cleaner.df["date_of_birth"][2]))
        self.assertTrue(pd.isna(self.cleaner.df["join_date"][2]))

    def test_validate_company(self):
        """
        Test the validate_company method.
        """
        self.cleaner.validate_company("company")
        expected = ["OpenAI", np.nan, "Microsoft", np.nan]
        print(
            "self.cleaner.df['company'].tolist():", self.cleaner.df["company"].tolist()
        )
        self.assertEqual(self.cleaner.df["company"].tolist(), expected)

    def test_validate_emails(self):
        """
        Test the validate_emails method.
        """
        self.cleaner.validate_emails("email_address")
        expected = ["john.doe@example.com", "jane@example.com", np.nan, np.nan]
        self.assertEqual(self.cleaner.df["email_address"].tolist(), expected)

    def test_validate_address(self):
        """
        Test the validate_address method.
        """
        self.cleaner.validate_address("address")
        expected = ["123 Main Street, Apt 4B", "456 Oak Avenue", np.nan, np.nan]
        self.assertEqual(self.cleaner.df["address"].tolist(), expected)

    def test_validate_country(self):
        """
        Test the validate_country method.
        """
        self.cleaner.validate_country("country")
        expected = pd.Series(["United States", "Germany", pd.NA, pd.NA])
        # Replace pd.NA with a placeholder to allow comparison
        result = self.cleaner.df["country"].fillna("pd.NA")
        expected_filled = expected.fillna("pd.NA")
        # Check if all values match, including pd.NA placeholders
        self.assertTrue(
            (result == expected_filled).all(),
            f"Expected: {expected_filled}, but got: {result}",
        )

    def test_validate_country_codes(self):
        """
        Test the validate_country_codes method.
        """
        self.cleaner.validate_country_codes("country_code")
        expected = ["US", "DE", "GB", np.nan]
        self.assertEqual(self.cleaner.df["country_code"].tolist(), expected)

    def test_validate_phone_number(self):
        """
        Test the validate_phone_number method.
        """
        self.cleaner.validate_phone_number("phone_number")
        expected = ["0123456789", "0044123456789", "0789456", np.nan]
        self.assertEqual(self.cleaner.df["phone_number"].tolist(), expected)

    def test_validate_user_uuid(self):
        """
        Test the validate_user_uuid method.
        """
        self.cleaner.validate_user_uuid("user_uuid")
        self.assertTrue(pd.notna(self.cleaner.df["user_uuid"][0]))
        self.assertTrue(pd.isna(self.cleaner.df["user_uuid"][2]))


if __name__ == "__main__":
    unittest.main()
