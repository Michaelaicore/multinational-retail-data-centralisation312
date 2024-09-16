"""
Module: Data Cleaning and Validation Models

This module contains Pydantic models for validating and cleaning user data, 
payment information, store details, product data, order data, and date-related information. 
Additionally, it includes a `DataCleaning` class that validates and cleans data 
using these models, logging any errors that occur during the validation process.

Classes:
    - UserModel: Pydantic model for user data validation.
    - PaymentModel: Pydantic model for payment data validation.
    - StoreModel: Pydantic model for store data validation.
    - ProductModel: Pydantic model for product data validation.
    - OrderModel: Pydantic model for order data validation.
    - DateModel: Pydantic model for date-related data validation.
    - DataCleaning: Class for validating and cleaning data, with error logging functionality.

Usage:
    This module can be used to validate and clean different types of data, ensuring
    that the data adheres to the specified format and constraints.
"""

import json
import os
import logging
import re
import uuid
from datetime import date

from dateutil import parser
import pandas as pd
from typing import Optional, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    EmailStr,
    conint,
    constr,
    field_validator,
    ValidationError,
    Field,
    condecimal,
)


class UserModel(BaseModel):
    """
    Pydantic model for validating user data.

    Attributes:
        index (int): Index of the user data.
        first_name (str): First name of the user, minimum length of 1 character.
        last_name (str): Last name of the user, minimum length of 1 character.
        date_of_birth (date): Date of birth of the user.
        company (str): Company name associated with the user.
        email_address (EmailStr): Email address of the user.
        address (str): Address of the user.
        country (str): Country of the user.
        country_code (str): 2-letter country code of the user's country.
        phone_number (str): Phone number of the user.
        join_date (date): Date when the user joined.
        user_uuid (UUID): UUID of the user.

    Validators:
        - parse_dates: Validates and parses date fields.
        - clean_phone_number: Cleans the phone number by removing unwanted characters.
        - validate_email_address: Validates and corrects email addresses.
        - validate_country_code: Validates and corrects the country code.
    """

    index: int
    first_name: constr(min_length=1)
    last_name: constr(min_length=1)
    date_of_birth: date
    company: str
    email_address: EmailStr
    address: str
    country: str
    country_code: constr(min_length=2, max_length=2)
    phone_number: str
    join_date: date
    user_uuid: uuid.UUID

    @field_validator("date_of_birth", "join_date", mode="before")
    def parse_dates(cls, value):
        """
        Convert string dates to date objects.

        Args:
        - value: The date value that may be in string format.

        Returns:
        - date: The parsed date object if valid, otherwise raises a ValueError.
        """
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except (ValueError, TypeError):
                raise ValueError(f"Invalid date format: {value}")
        return value

    @field_validator("phone_number", mode="before")
    def clean_phone_number(cls, value):
        """
        Clean the phone number by removing unwanted characters and formatting it.

        Args:
        - value: The phone number string to be cleaned.

        Returns:
        - str: The cleaned phone number.
        """
        if isinstance(value, str):
            value = re.sub(r"^\+\d{2,3}\(0\)|^001[- ]?", "", value)
            value = re.sub(r"^\(0\)|^\(00\)|\(", "", value)
            value = re.sub(r"\)", "", value)
            value = re.sub(r"^\d{2,3}", "", value)
            value = (
                value.replace("-", ", ")
                .replace(".", ", ")
                .replace("x", " ext ")
                .strip()
            )
        return value

    @field_validator("email_address", mode="before")
    def validate_email_address(cls, value):
        """
        Validate the email address by correcting common errors like double '@' signs.

        Args:
        - value: The email address string to be validated.

        Returns:
        - str: The corrected email address.
        """
        if isinstance(value, str):
            value = value.replace("@@", "@")
        return value

    @field_validator("country_code", mode="before")
    def validate_country_code(cls, value, info):
        """
        Validate and adjust the country code based on the country name.

        Args:
        - value: The country code string.
        - info: Additional model information, used to retrieve the country name.

        Returns:
        - str: The validated and possibly adjusted country code.
        """
        country = info.data.get("country")
        if country == "United Kingdom" and value == "GGB":
            return "GB"
        return value

    class Config:
        str_strip_whitespace = True


class PaymentModel(BaseModel):
    """
    Pydantic model for validating payment data.

    Attributes:
        card_number (str): Credit or debit card number.
        expiry_date (str): Expiry date of the card in the format MM/YY.
        card_provider (str): The card provider (e.g., Visa, MasterCard).
        date_payment_confirmed (date): Date when the payment was confirmed.

    Validators:
        - validate_expiry_date: Validates the expiry date format.
        - validate_card_number: Validates and cleans the card number.
        - validate_and_clean_date_payment: Validates and parses the date of payment confirmation.
    """

    card_number: str
    expiry_date: str
    card_provider: str
    date_payment_confirmed: date

    @field_validator("expiry_date")
    def validate_expiry_date(cls, value):
        """
        Validate the format of the expiry date, ensuring it's in MM/YY format.

        Args:
        - value: The expiry date string to be validated.

        Returns:
        - str: The validated expiry date if the format is correct, otherwise raises a ValueError.
        """
        if not re.match(r"^(0[1-9]|1[0-2])\/([0-9]{2})$", value):
            raise ValueError("Invalid expiry date format. Expected MM/YY.")
        return value

    @field_validator("card_number", mode="before")
    def validate_card_number(cls, value):
        """
        Validate and clean the card number by removing non-digit characters.

        Args:
        - value: The card number string or integer.

        Returns:
        - str: The cleaned card number as a string.
        """
        # Ensure the value is a string or can be converted to a string
        if isinstance(value, int):
            value = str(value)
        elif not isinstance(value, str):
            raise ValueError("Card number must be a string or convertible to a string.")

        # Clean the card number by removing non-digit characters
        return re.sub(r"\D", "", value)

        # value = re.sub(r'\D', '', value).lstrip('?')
        # if not value.isdigit() or not (9 <= len(value) <= 19):
        #     raise ValueError("Card number must contain only digits and be between 13 and 19 digits.")
        # return str(value)

    @field_validator("date_payment_confirmed", mode="before")
    def validate_and_clean_date_payment(cls, value):
        """
        Convert string dates to date objects for date_payment_confirmed.

        Args:
        - value: The date value that may be in string format.

        Returns:
        - date: The parsed date object if valid, otherwise raises a ValueError.
        """
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except (ValueError, TypeError):
                raise ValueError(f"Invalid date format: {value}")
        return value

    class Config:
        str_strip_whitespace = True


class StoreModel(BaseModel):
    """
    Pydantic model for validating store data.

    Attributes:
        index (int): Index of the store data, must be greater than or equal to 1.
        address (str): Address of the store.
        longitude (Optional[float]): Longitude of the store's location.
        locality (Optional[str]): Locality of the store, can be missing.
        store_code (str): Store code, must have a minimum length of 1 character.
        staff_numbers (int): Number of staff members, must be non-negative.
        opening_date (date): Opening date of the store in the format YYYY-MM-DD.
        store_type (str): Type of the store.
        latitude (float): Latitude of the store's location.
        country_code (str): 2-letter country code of the store's country.
        continent (str): Continent where the store is located.

    Validators:
        - validate_open_date: Validates and parses the opening date.
        - validate_staff_numbers: Validates and cleans the staff numbers.
    """

    index: conint(ge=1)  # Integer, greater than or equal to 1
    address: str  # String, required
    longitude: Optional[float]  # Optional float for longitude
    lat: Optional[object]
    locality: Optional[str]  # Optional string, locality can be missing
    store_code: constr(min_length=1)  # Store code, required with minimum length 1
    staff_numbers: conint(ge=0)  # Integer, number of staff should be non-negative
    opening_date: date  # Date field in the format YYYY-MM-DD
    store_type: str  # Store type, required
    latitude: float  # Float, latitude is required
    country_code: constr(min_length=2, max_length=2)  # 2-letter country code
    continent: str  # Continent, required

    @field_validator("opening_date", mode="before")
    def validate_open_date(cls, value):
        """
        Convert string dates to date objects for the opening_date field.

        Args:
        - value: The date value that may be in string format.

        Returns:
        - date: The parsed date object if valid, otherwise raises a ValueError.
        """
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except (ValueError, TypeError):
                raise ValueError(f"Invalid date format: {value}")
        return value

    @field_validator("staff_numbers", mode="before")
    def validate_staff_numbers(cls, value):
        """
        Validate and clean the staff numbers by removing non-digit characters.

        Args:
        - value: The staff numbers field, which may contain non-digit characters.

        Returns:
        - int: The cleaned and converted staff number as an integer.
        """
        # Remove any non-digit characters
        cleaned_value = "".join(filter(str.isdigit, str(value)))

        # If cleaned_value isn't a valid number, raise a ValueError
        if not cleaned_value.isdigit():
            raise ValueError(f"Invalid staff number: {value}")

        # Convert the cleaned value to an integer
        return int(cleaned_value)

    class Config:
        # Allow field aliases to be used when populating the model
        populate_by_name = True
        str_strip_whitespace = True


class ProductModel(BaseModel):
    """
    Pydantic model for validating product data.

    Attributes:
        product_name (str): Name of the product.
        product_price (str): Price of the product.
        weight (str): Weight of the product.
        category (str): Category of the product.
        EAN (str): European Article Number (EAN) of the product.
        date_added (str): Date when the product was added.
        uuid (str): UUID of the product.
        removed (str): Status indicating whether the product is still available or removed.
        product_code (str): Code of the product.

    Validators:
        - validate_product_price: Validates and cleans the product price.
        - validate_weight: Validates and converts the weight into grams.
        - validate_date_added: Validates and parses the date the product was added.
        - validate_EAN: Validates the EAN, ensuring it contains only digits.
        - validate_uuid: Validates the UUID, ensuring it contains exactly 16 digits.
        - validate_removed: Validates the removal status of the product.
    """

    product_name: str
    product_price: str
    weight: str
    category: str
    EAN: str
    date_added: str
    uuid: str
    removed: str
    product_code: str

    @field_validator("product_price")
    def validate_product_price(cls, value):
        """
        Validate and clean the product price by removing currency symbols and converting to float.

        Args:
        - value: The product price string.

        Returns:
        - float: The cleaned and converted product price as a float.
        """
        # Remove currency symbol and convert to float
        if value.startswith("£"):
            value = value[1:]
        try:
            return float(value)
        except ValueError:
            raise ValueError("Invalid product price format")

    @field_validator("weight")
    def validate_weight(cls, value):
        """
        Validate and convert product weight into grams, handling various units.

        Args:
        - value: The weight string which may contain units like kg, g, ml, or oz.

        Returns:
        - float: The converted weight in grams.
        """
        # Convert non-standard units to grams
        if isinstance(value, str):
            value = value.strip().lower()
            if "ml" in value:
                # Assuming 1ml = 1g
                return float(
                    re.sub("[^0-9.]", "", value)
                )  # Remove non-numeric characters
            elif "kg" in value:
                return float(re.sub("[^0-9.]", "", value)) * 1000  # Convert kg to g
            elif "g" in value:
                return float(re.sub("[^0-9.]", "", value))
            elif "oz" in value:
                # Convert ounces to grams
                return float(re.sub("[^0-9.]", "", value)) * 28.3495
            else:
                raise ValueError("Weight must be in kg or g")
        return value

    @field_validator("date_added")
    def validate_date_added(cls, value):
        """
        Convert string dates to date objects for the date_added field.

        Args:
        - value: The date value that may be in string format.

        Returns:
        - date: The parsed date object if valid, otherwise raises a ValueError.
        """
        if isinstance(value, str):
            try:
                return parser.parse(value).date()
            except (ValueError, TypeError):
                raise ValueError(f"Invalid date format: {value}")
        return value

    @field_validator("EAN")
    def validate_EAN(cls, value):
        """
        Validate the EAN (European Article Number) to ensure it contains only digits.

        Args:
        - value: The EAN as a string.

        Returns:
        - str: The validated EAN if it contains only digits, otherwise raises a ValueError.
        """
        if not re.match(r"^\d+$", value):
            raise ValueError("EAN should only contain digits")
        return value

    @field_validator("uuid")
    def validate_uuid(cls, value):
        """
        Validate the UUID to ensure it is exactly 16 digits.

        Args:
        - value: The UUID as a string or integer.

        Returns:
        - str: The validated UUID if it is exactly 16 digits, otherwise raises a ValueError.
        """
        value_str = str(value)  # Ensure value is treated as a string
        if len(value_str) != 16 or not value_str.isdigit():
            raise ValueError("Card number must be exactly 16 digits.")
        return value_str

    @field_validator("removed")
    def validate_removed(cls, value):
        """
        Validate the removed status to ensure it is either 'Still_avaliable' or 'Removed'.

        Args:
        - value: The removed status as a string.

        Returns:
        - str: The validated status if it is valid, otherwise raises a ValueError.
        """
        if value not in ["Still_avaliable", "Removed"]:
            raise ValueError("Removed status should be 'Still_avaliable' or 'Removed'")
        return value

    class Config:
        # Allow field aliases to be used when populating the model
        populate_by_name = True
        str_strip_whitespace = True


class OrderModel(BaseModel):
    """
    Pydantic model for validating order data.

    Attributes:
        level_0 (Optional[int]): Optional field for level_0.
        index (Optional[int]): Optional field for the index.
        date_uuid (UUID): UUID representing the date of the order.
        user_uuid (UUID): UUID of the user who placed the order.
        card_number (str): Card number used for the order.
        store_code (Optional[str]): Code of the store where the order was placed.
        product_code (Optional[str]): Code of the product in the order.
        product_quantity (Optional[Decimal]): Quantity of the product in the order.

    Validators:
        - validate_card_number: Validates the card number, ensuring it has the correct length.
        - validate_product_quantity: Validates the product quantity, ensuring it is greater than 0.
        - validate_uuids: Validates the UUID fields.
    """

    level_0: Optional[int] = None
    index: Optional[int] = None
    date_uuid: UUID
    user_uuid: UUID
    card_number: str
    store_code: Optional[str] = None
    product_code: Optional[str] = None
    product_quantity: Optional[condecimal(max_digits=10, decimal_places=2)] = None

    @field_validator("card_number", mode="before")
    def validate_card_number(cls, value):
        """
        Validate and clean the card number to ensure it is between 9 and 16 digits.

        Args:
        - value: The card number string or integer.

        Returns:
        - str: The cleaned card number as a string if it meets the length requirement, otherwise raises a ValueError.
        """
        value_str = str(value)
        if not (9 <= len(value_str) <= 19):
            raise ValueError("Card number must be between 9 and 16 digits.")
        return value_str

    @field_validator("product_quantity", mode="before")
    def validate_product_quantity(cls, value):
        """
        Validate the product quantity to ensure it is greater than 0.

        Args:
        - value: The product quantity.

        Returns:
        - condecimal: The validated product quantity if greater than 0, otherwise raises a ValueError.
        """
        if value and value <= 0:
            raise ValueError("Product quantity must be greater than 0.")
        return value

    @field_validator("date_uuid", "user_uuid", mode="before")
    def validate_uuids(cls, value):
        """
        Validate UUIDs to ensure they are valid UUID version 4 strings.

        Args:
        - value: The UUID string.

        Returns:
        - UUID: The validated UUID if valid, otherwise raises a ValueError.
        """
        try:
            return UUID(str(value))
        except ValueError as e:
            raise ValueError(f"Invalid UUID: {value}") from e

    class Config:
        # Allow field aliases to be used when populating the model
        populate_by_name = True
        str_strip_whitespace = True


class DateModel(BaseModel):
    """
    Pydantic model for validating date-related data.

    Attributes:
        timestamp (str): Timestamp in the format HH:MM:SS.
        month (int): Month of the year, must be between 1 and 12.
        year (int): Year, must be between 1900 and 2100.
        day (int): Day of the month, must be between 1 and 31.
        time_period (Literal): Time period of the day (e.g., Morning, Midday, Evening, Late_Hours).
        date_uuid (UUID): UUID representing the date.

    Validators:
        - validate_timestamp: Validates the timestamp format.
        - validate_day: Validates the day, ensuring it is valid for the given month and year.
    """

    timestamp: str
    month: conint(ge=1, le=12)  # Ensuring month is between 1 and 12
    year: conint(ge=1900, le=2100)  # Valid year range
    day: conint(ge=1, le=31)  # Ensuring day is between 1 and 31
    time_period: Literal["Morning", "Midday", "Evening", "Late_Hours"]
    date_uuid: uuid.UUID

    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        """
        Validate the timestamp to ensure it is in the correct format HH:MM:SS.

        Args:
        - value: The timestamp string.

        Returns:
        - str: The validated timestamp if in correct format, otherwise raises a ValueError.
        """
        try:
            datetime.strptime(value, "%H:%M:%S")  # Check if the time format is valid
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {value}")
        return value

    @classmethod
    def validate_day(cls, value: int, month: int, year: int) -> int:
        """
        Validate the day to ensure it is a valid day for the given month and year.

        Args:
        - value: The day of the month.
        - month: The month of the year.
        - year: The year.

        Returns:
        - int: The validated day if it is valid for the given month and year, otherwise raises a ValueError.
        """
        try:
            datetime(year, month, value)
        except ValueError:
            raise ValueError(
                f"Invalid day: {value} for month: {month} and year: {year}"
            )
        return value

    class Config:
        str_min_length = 1
        str_strip_whitespace = True


class DataCleaning:
    """
    Class for validating and cleaning data using Pydantic models.

    Attributes:
        df (pd.DataFrame): DataFrame containing the data to be cleaned.
        valid_data (list): List of valid data rows after validation.
        invalid_data (list): List of invalid data rows after validation.
        invalid_errors (list): List of validation errors encountered during the cleaning process.
        model_class (BaseModel): Pydantic model class used for validation.
        class_name (str): Name of the DataCleaning class instance.

    Methods:
        _setup_logger(): Sets up a logger for recording validation errors.
        validate_and_clean_data(): Validates and cleans the data, storing valid and invalid rows separately.
        get_valid_data(): Returns a DataFrame of valid data.
        get_invalid_data(): Returns a DataFrame of invalid data.
        save_invalid_data_log(log_filename): Saves invalid data along with errors to a CSV log file.
    """

    def __init__(self, model_class=UserModel, class_name="Data_cleaning"):
        """
        Initialize the DataCleaning object with a Pydantic model class and class name.

        Args:
        - model_class: The Pydantic model class used for validation (default: UserModel).
        - class_name: The name of the class for logging purposes (default: 'Data_cleaning').
        """
        self.df = None
        self.valid_data = None
        self.invalid_data = None
        self.invalid_errors = None
        self.model_class = model_class
        # self.project_name = project_name
        # self.module_name = module_name
        self.class_name = class_name
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """
        Set up a logger for recording errors during the data validation process.

        Returns:
        - logger: A configured logger object.
        """
        log_path = os.path.join(os.getcwd(), "logging", self.model_class.__name__)
        os.makedirs(log_path, exist_ok=True)
        log_filename = os.path.join(log_path, f"{self.class_name}_errors.log")
        logger = logging.getLogger(self.class_name)
        handler = logging.FileHandler(log_filename)
        handler.setLevel(logging.ERROR)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.ERROR)
        return logger

    def validate_and_clean_data(self):
        """
        Validate and clean the data in the DataFrame using the specified Pydantic model.

        This method iterates over each row in the DataFrame, validates the data
        using the Pydantic model, and separates the valid and invalid data.
        Invalid data is logged with corresponding errors.
        """
        self.logger = self._setup_logger()
        self.valid_data = []
        self.invalid_data = []
        self.invalid_errors = []
        for _, row in self.df.iterrows():
            try:
                model_instance = self.model_class(**row.to_dict())
                self.valid_data.append(model_instance.dict())
            except ValidationError as e:
                self.invalid_data.append(row)
                row_index = row.get("index", row.name)
                self.invalid_errors.append(
                    {"index": row_index, "errors": e.errors(), "data": row.to_dict()}
                )
                self.logger.error(
                    f"Validation error at index {row_index}: {e.errors()}"
                )

    def get_valid_data(self):
        """
        Retrieve the valid data after validation.

        Returns:
        - DataFrame: A DataFrame containing valid data records.
        """
        self.valid_data = pd.DataFrame(self.valid_data)
        return pd.DataFrame(self.valid_data)

    def get_invalid_data(self):
        """
        Retrieve the invalid data after validation.

        Returns:
        - DataFrame: A DataFrame containing invalid data records.
        """
        return pd.DataFrame(self.invalid_data)

    def save_invalid_data_log(self, log_filename=None):
        """
        Save the invalid data records to a CSV log file.

        Args:
        - log_filename: The filename for saving the invalid data (default:
          logs the data in a logging directory under the current working directory).
        """
        log_filename = log_filename or os.path.join(
            os.getcwd(),
            "logging",
            self.model_class.__name__,
            f"{self.class_name}_invalid_data.csv",
        )
        # Extract the directory path from the filename
        log_directory = os.path.dirname(log_filename)

        # Ensure the directory exists
        os.makedirs(log_directory, exist_ok=True)
        if self.invalid_data:
            pd.DataFrame(self.invalid_data).to_csv(log_filename, index=False)


if __name__ == "__main__":

    # clean and validate Store_data
    cleaner = DataCleaning(model_class=StoreModel)
    df = pd.read_csv("store_data.csv")
    cleaner.df = df.drop(columns=["lat"])
    cleaner.validate_and_clean_data()
    valid_df = cleaner.get_valid_data()
    invalid_df = cleaner.get_invalid_data()
    cleaner.save_invalid_data_log()
    print("Valid data:")
    print(valid_df.head())
    print("\nInvalid data:")
    print(invalid_df)

    # clean and validate product_data
    cleaner = DataCleaning(model_class=ProductModel)
    cleaner.df = pd.read_csv("product_table.csv")
    cleaner.df.drop(columns=["Unnamed: 0"], errors="ignore")
    cleaner.validate_and_clean_data()
    valid_df = cleaner.get_valid_data()
    invalid_df = cleaner.get_invalid_data()
    cleaner.save_invalid_data_log()
    print("Valid data:")
    print(valid_df.head())
    print("\nInvalid data:")
    print(invalid_df)

    cleaner = DataCleaning(model_class=OrderModel)
    cleaner.df = pd.read_csv("oreder_table.csv")
    columns_to_remove = ["first_name", "last_name", "1"]
    cleaner.df.drop(columns=columns_to_remove, errors="ignore")
    cleaner.validate_and_clean_data()
    valid_df = cleaner.get_valid_data()
    invalid_df = cleaner.get_invalid_data()
    cleaner.save_invalid_data_log()
    print("Valid data:")
    print(valid_df.head())
    print("\nInvalid data:")
    print(invalid_df)

    cleaner = DataCleaning(model_class=DateModel)
    with open("date_json.json", "r") as file:
        data = json.load(file)
    cleaner.df = pd.DataFrame(data[0])

    # cleaner.df.drop(columns=['Unnamed: 0'], errors='ignore')
    cleaner.validate_and_clean_data()
    valid_df = cleaner.get_valid_data()
    invalid_df = cleaner.get_invalid_data()
    cleaner.save_invalid_data_log()
    print("Valid data:")
    print(valid_df.head())
    print("\nInvalid data:")
    print(invalid_df)
