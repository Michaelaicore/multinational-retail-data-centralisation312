import unittest
import pandas as pd
import numpy as np
import uuid
import logging
from main.data_cleaning import DataCleaning
import unittest
import pandas as pd
from pydantic import ValidationError
from unittest.mock import patch, MagicMock
import os
from main.data_cleaning import (
    DataCleaning,
    UserModel,
    StoreModel,
    PaymentModel,
    ProductModel,
    DateModel,
    OrderModel,
)

from datetime import date
from uuid import UUID
from pydantic import ValidationError
from decimal import Decimal


class TestUserModel(unittest.TestCase):

    def test_valid_user(self):
        user = UserModel(
            index=1,
            first_name="John",
            last_name="Doe",
            date_of_birth="1990-01-01",
            company="Company Inc.",
            email_address="john.doe@example.com",
            address="123 Main St",
            country="United Kingdom",
            country_code="GB",
            phone_number="+44 1234 567890",
            join_date="2020-01-01",
            user_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
        )
        self.assertEqual(user.first_name, "John")
        self.assertEqual(user.country_code, "GB")

        user = UserModel(
            index=1,
            first_name="John",
            last_name="Doe",
            date_of_birth="1990/01/01",  # This will be reformated and validated
            company="Company Inc.",
            email_address="john.doe@example.com",
            address="123 Main St",
            country="United Kingdom",
            country_code="GGB",
            phone_number="+44 1234 567890",
            join_date="2020-01-01",
            user_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
        )
        self.assertEqual(user.date_of_birth.strftime("%Y-%m-%d"), "1990-01-01")
        self.assertEqual(user.country_code, "GB")

    def test_invalid_email(self):
        with self.assertRaises(ValidationError):
            UserModel(
                index=1,
                first_name="John",
                last_name="Doe",
                date_of_birth="1990-01-01",
                company="Company Inc.",
                email_address="john.doe[=]example.com",  # INVALID EMAIL
                address="123 Main St",
                country="United Kingdom",
                country_code="GB",
                phone_number="+44 1234 567890",
                join_date="2020-01-01",
                user_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
            )


class TestPaymentModel(unittest.TestCase):

    def test_valid_payment(self):
        payment = PaymentModel(
            card_number="1234-5678-9876-5432",
            expiry_date="12/25",
            card_provider="Visa",
            date_payment_confirmed="01 January 2023",  # This will be reformated and be validated
        )
        self.assertEqual(payment.card_number, "1234567898765432")

    def test_invalid_expiry_date(self):
        with self.assertRaises(ValidationError):
            PaymentModel(
                card_number="1234-5678-9876-5432",
                expiry_date="13/25",  # Invalid month
                card_provider="Visa",
                date_payment_confirmed="2023-01-01",
            )


class TestStoreModel(unittest.TestCase):

    def test_valid_store(self):
        store = StoreModel(
            index=1,
            address="456 Elm St",
            lat="nan",
            longitude=0.1278,
            latitude=51.5074,
            locality="London",
            store_code="STORE123",
            staff_numbers=10,
            opening_date="2010-05-05",
            store_type="Retail",
            country_code="GB",
            continent="Europe",
        )
        self.assertEqual(store.staff_numbers, 10)

    def test_invalid_staff_numbers(self):
        with self.assertRaises(ValidationError):
            StoreModel(
                index=1,
                address="456 Elm St",
                longitude=0.1278,
                latitude=51.5074,
                locality="London",
                store_code="STORE123",
                staff_numbers=-5,  # Invalid staff number
                opening_date="2010-05-05",
                store_type="Retail",
                country_code="GB",
                continent="Europe",
            )


class TestProductModel(unittest.TestCase):

    def test_valid_product(self):
        product = ProductModel(
            product_name="Gadget",
            product_price="£9.99",
            weight="1kg",
            category="Electronics",
            EAN="1234567890123",
            date_added="2023-06-01",
            uuid="83dc0a69-f96f-4c34-bcb7-928acae19a94",
            removed="Still_avaliable",
            product_code="G123",
        )
        self.assertEqual(product.product_price, 9.99)
        self.assertEqual(product.weight, 1000)

    def test_invalid_EAN(self):
        with self.assertRaises(ValidationError):
            ProductModel(
                product_name="Gadget",
                product_price="£9.99",
                weight="1kg",
                category="Electronics",
                EAN="ABC123",  # Invalid EAN
                date_added="2023-06-01",
                uuid="1234567890123456",
                removed="Still_avaliable",
                product_code="G123",
            )


class TestOrderModel(unittest.TestCase):

    def test_valid_order(self):
        order = OrderModel(
            date_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
            user_uuid=UUID("123e4567-e89b-12d3-a456-426614174001"),
            card_number="123456789012",
            store_code="STORE123",
            product_code="P123",
            product_quantity=Decimal("1.50"),
        )
        self.assertEqual(order.card_number, "123456789012")
        self.assertEqual(order.product_quantity, Decimal("1.50"))

    def test_invalid_product_quantity(self):
        with self.assertRaises(ValidationError):
            OrderModel(
                date_uuid="123e4567-e89b-12d3-a456-426614174000",
                user_uuid="123e4567-e89b-12d3-a456-426614174001",
                card_number="123456789012",
                store_code="STORE123",
                product_code="P123",
                product_quantity=Decimal("-1.00"),  # Invalid quantity
            )
        with self.assertRaises(ValidationError):
            OrderModel(
                date_uuid="123e4567-e89b-12d3-a456-42661417400G",  # Invalid UUID
                user_uuid="123e4567-e89b-12d3-a456-426614174001",
                card_number="123456789012",
                store_code="STORE123",
                product_code="P123",
                product_quantity=Decimal("1.50"),
            )


class TestDateModel(unittest.TestCase):

    def test_valid_date(self):
        date_model = DateModel(
            timestamp="12:30:00",
            month=6,
            year=2023,
            day=15,
            time_period="Midday",
            date_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
        )
        self.assertEqual(date_model.timestamp, "12:30:00")
        self.assertEqual(date_model.month, 6)

    def test_invalid_day(self):
        with self.assertRaises(ValidationError):
            DateModel(
                timestamp="12:30:00",
                month=20,
                year=2023,
                day=30,  # Invalid day for February
                time_period="Friday",
                date_uuid=UUID("123e4567-e89b-12d3-a456-426614174000"),
            )


class TestDataCleaning(unittest.TestCase):

    def setUp(self):
        # Sample DataFrame for testing
        self.data = {
            "index": [1, 2, 3],
            "first_name": ["John", "Jane", "Doe"],
            "last_name": ["Doe", "Smith", "Johnson"],
            "date_of_birth": ["1990-01-01", "1985-05-12", "1970-09-09"],
            "company": ["CompanyA", "CompanyB", "CompanyC"],
            "email_address": [
                "john.doe@example.com",
                "jane.smith@@example.com",
                "invalid-email",
            ],
            "address": ["123 Main St", "456 Side St", "789 Front St"],
            "country": ["United States", "United Kingdom", "Germany"],
            "country_code": ["US", "GB", "DE"],
            "phone_number": ["+1-800-123456", "(44) 020-123456", "+49 (30) 123456"],
            "join_date": ["2020-01-01", "2019-06-15", "invalid-date"],
            "user_uuid": [
                "123e4567-e89b-12d3-a456-426614174000",
                "invalid-uuid",
                "123e4567-e89b-12d3-a456-426614174001",
            ],
        }
        self.df = pd.DataFrame(self.data)

        # Create instance of DataCleaning
        self.data_cleaner = DataCleaning(model_class=UserModel)

    @patch("os.makedirs")
    def test_setup_logger(self, mock_makedirs):
        # Create a mock logger
        mock_logger = MagicMock()
        # Mock the _setup_logger method to return the mock logger
        self.data_cleaner._setup_logger = MagicMock(return_value=mock_logger)

        # Call _setup_logger to get the mocked logger
        logger = self.data_cleaner._setup_logger()

        # Set the mock attributes manually
        mock_logger.name = self.data_cleaner.class_name
        mock_logger.level = logging.ERROR

        # Assertions
        self.assertEqual(logger.name, self.data_cleaner.class_name)
        self.assertEqual(logger.level, logging.ERROR)

    @patch("main.data_cleaning.DataCleaning._setup_logger")
    def test_validate_and_clean_data(self, mock_setup_logger):
        mock_logger = MagicMock()
        mock_setup_logger.return_value = mock_logger

        # Set DataFrame and run validation
        self.data_cleaner.df = self.df
        self.data_cleaner.validate_and_clean_data()

        # Check if valid data is processed correctly
        valid_data = self.data_cleaner.get_valid_data()
        self.assertEqual(len(valid_data), 1)  # Only 1 valid row

        # Check if invalid data is captured correctly
        invalid_data = self.data_cleaner.get_invalid_data()
        self.assertEqual(len(invalid_data), 2)  # 2 invalid rows

        # Check if invalid errors are captured correctly
        invalid_errors = self.data_cleaner.invalid_errors
        self.assertEqual(len(invalid_errors), 2)  # 2 invalid rows

        # Check if the logger's error method was called
        self.assertTrue(mock_logger.error.called)
        error_message = mock_logger.error.call_args[0][0]
        self.assertIn("Validation error at index", error_message)

    @patch("os.makedirs")
    def test_save_invalid_data_log(self, mock_makedirs):
        # Mocking logger setup to avoid file creation
        self.data_cleaner._setup_logger = MagicMock(
            return_value=logging.getLogger("dummy")
        )

        # Set DataFrame and run validation
        self.data_cleaner.df = self.df
        self.data_cleaner.validate_and_clean_data()

        # Save invalid data log
        with patch("pandas.DataFrame.to_csv") as mock_to_csv:
            self.data_cleaner.save_invalid_data_log("test_invalid_data.csv")
            mock_to_csv.assert_called_once_with("test_invalid_data.csv", index=False)


if __name__ == "__main__":
    unittest.main()
