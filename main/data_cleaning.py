"""
Data Cleaning Module

This module provides a `DataCleaning` class for validating and cleaning data in a pandas DataFrame.
The `DataCleaning` class includes methods to validate and clean various types of data, such as names, 
dates, email addresses, phone numbers, and more. The class uses a decorator to apply validation 
functions to specific columns and reports invalid entries along with their original DataFrame rows.

Example usage:
    df = pd.read_csv('data.csv')
    country_code = {'Germany':'DE', 'United Kingdom':'GB', 'United States':'US'}
    data_cleaner = DataCleaning(df, country_code)
    data_cleaner.run()

Author:
    Your Name (youremail@example.com)
Version:
    1.0
"""

import re
import numpy as np
import pandas as pd
import regex as reg
import uuid


def apply_to_column(func):
    """
    Decorator that applies a validation function to a specified column in a DataFrame.

    Args:
        func (function): The validation function to apply.

    Returns:
        function: A wrapper function that applies the validation and identifies invalid entries.
    """

    def wrapper(self, col_name, *args, **kwargs):
        original_df = self.df.copy()  # Create a copy of the entire original DataFrame
        self.df[col_name] = self.df[col_name].apply(
            lambda x: func(self, str(x) if pd.notna(x) else x, *args, **kwargs)
        )

        # Identify the indices of the values that became NaN after applying the function
        invalid_indices = original_df[self.df[col_name].isna()].index.tolist()

        if invalid_indices:
            print(
                f"Invalid values in column '{col_name}' at indices {invalid_indices}:\n"
            )
            print("Original DataFrame rows with invalid values:\n")
            print(
                original_df.loc[invalid_indices]
            )  # Print the rows from the original DataFrame

    return wrapper


class DataCleaning:
    """
    A class used to clean and validate data within a pandas DataFrame.

    Attributes:
        df (pd.DataFrame): The DataFrame containing the data to be cleaned.
        country_code (dict): A dictionary mapping country names to their corresponding\
              country codes.

    Methods:
        validate_index(value: str) -> float:
            Validates that an index value is a non-negative numeric value.

        validate_name(name: str) -> str:
            Validates and cleans a name string.

        validate_date(date_str: str) -> pd.Timestamp or pd.NaT:
            Validates and converts a date string to a pandas datetime object.

        validate_company(company_name: str) -> str:
            Validates and cleans a company name.

        validate_emails(email: str) -> str:
            Validates and cleans an email address.

        validate_address(address: str) -> str:
            Validates and cleans an address string.

        validate_country(country: str) -> str:
            Validates a country name against a list of valid countries.

        validate_country_codes(country_code: str) -> str:
            Validates a country code, ensuring it is a two-letter uppercase code.

        validate_phone_number(phone_number: str) -> str:
            Validates and standardizes a phone number.

        validate_user_uuid(uuid_str: str) -> str:
            Validates if a string is a valid UUID version 4.

        run():
            Runs all validation methods on the DataFrame.
    """

    def __init__(self, df, country_code):
        """
        Constructs all the necessary attributes for the DataCleaning object.

        Args:
            df (pd.DataFrame): The DataFrame containing the data to be cleaned.
            country_code (dict): A dictionary mapping country names to their corresponding country codes.
        """
        self.df = df
        self.country_code = country_code

    @apply_to_column
    def validate_index(self, value: str) -> float:
        """
        Validates that an index value is a non-negative numeric value.

        Args:
            value (str): The index value to validate.

        Returns:
            float or np.nan: The numeric index value if valid, otherwise NaN.
        """
        try:
            num = int(value)
            if num >= 0:
                return num
        except (ValueError, TypeError):
            return np.nan

    @apply_to_column
    def validate_name(self, name: str) -> str:
        """
        Validates a name string, allowing letters, hyphens, spaces, periods, and apostrophes.

        Args:
            name (str): The name string to validate.

        Returns:
            str or np.nan: The cleaned name if valid, otherwise NaN.
        """
        if isinstance(name, str) and reg.match(
            r"^\p{L}+(?:[-\s'\p{L}]+)*\.?$", name.strip()
        ):
            return name.strip()
        return np.nan

    @apply_to_column
    def validate_date(self, date_str):
        """
        Validates and converts a date string to a pandas datetime object.

        Args:
            date_str (str): The date string to validate.

        Returns:
            pd.Timestamp or pd.NaT: A pandas datetime object or NaT if invalid.
        """
        # Handle missing values explicitly
        if pd.isna(date_str):
            return pd.NaT

        # Attempt to parse the date string, with invalid parsing resulting in NaT
        return pd.to_datetime(date_str, errors="coerce", infer_datetime_format=True)

    @apply_to_column
    def validate_company(self, company_name: str) -> str:
        """
        Validates and cleans a company name.

        Args:
            company_name (str): The company name to validate.

        Returns:
            str or np.nan: The cleaned company name if valid, otherwise NaN.
        """
        if isinstance(company_name, str) and company_name.strip():
            return company_name.strip()
        return np.nan

    @apply_to_column
    def validate_emails(self, email: str) -> str:
        """
        Validates and cleans an email address.

        Args:
            email (str): The email address to validate.

        Returns:
            str or np.nan: The cleaned email address if valid, otherwise NaN.
        """
        if isinstance(email, str):
            email = email.replace("@@", "@")
            split_email = re.split(r"[@.]", email)
            if len(split_email) > 2 and not any(" " in item for item in split_email):
                return email
        return np.nan

    @apply_to_column
    def validate_address(self, address: str) -> str:
        """
        Validates and cleans an address string.

        Args:
            address (str): The address string to validate.

        Returns:
            str or np.nan: The cleaned address if valid, otherwise NaN.
        """
        if isinstance(address, str):
            # Replace newlines with commas
            address = address.replace("\n", ", ")
            # Strip extra whitespace and replace with a single space
            address = re.sub(r"\s+", " ", address.strip())
            # Replace common abbreviations
            replacements = {"St.": "Street", "Ave": "Avenue", "Rd.": "Road"}
            for abbr, full in replacements.items():
                address = address.replace(abbr, full)
            # Standardize casing
            address = address.title()
        return address if address else np.nan

    @apply_to_column
    def validate_country(self, country: str) -> str:
        """
        Validates a country name against a list of valid countries.

        Args:
            country (str): The country name to validate.
            valid_countries (list): A list of valid country names.

        Returns:
            str or pd.NA: The country name if valid, otherwise pd.NA.
        """

        if isinstance(country, str) and country in self.country_code.keys():
            print(f"Valid country: {country}")
            return country

        print(f"Invalid country: {country}")
        return pd.NA

    @apply_to_column
    def validate_country_codes(self, country_code) -> str:
        """
        Validates a country code, ensuring it is a two-letter uppercase code.

        Args:
            country_code (str): The country code to validate.

        Returns:
            str or np.nan: The country code if valid, otherwise NaN.
        """
        if isinstance(country_code, str) and re.match(r"^[A-Z]{2}$", country_code):
            return country_code
        elif country_code == "GGB":
            return "GB"
        return np.nan

    @apply_to_column
    def validate_phone_number(self, phone_number: str) -> str:
        """
        Validates and standardizes a phone number.

        Args:
            phone_number (str): The phone number to validate.

        Returns:
            str or np.nan: The standardized phone number if valid, otherwise NaN.
        """
        if isinstance(phone_number, str) and phone_number.strip():
            # Remove country code if present
            cleaned_number = re.sub(r"\+\d+(?=\()", "", phone_number)
            # Remove the specific "(0)" if it starts with it
            cleaned_number = re.sub(r"^\(0\)", "", cleaned_number)
            # Remove parentheses and dots
            cleaned_number = re.sub(r"[().]", "", cleaned_number)
            # Replace dots with spaces and strip whitespace
            cleaned_number = cleaned_number.replace(".", " ").strip()

            # Ensure the number starts with '0' if it's non-empty
            if not cleaned_number.startswith("0"):
                cleaned_number = "0" + cleaned_number

            return cleaned_number if cleaned_number else np.nan
        # Return np.nan for non-string or empty values
        return np.nan

    @apply_to_column
    def validate_user_uuid(self, uuid_str: str) -> str:
        """
        Validates if a string is a valid UUID version 4.

        Args:
            uuid_str (str): The UUID string to validate.

        Returns:
            str or np.nan: The UUID string if valid, otherwise NaN.
        """
        if isinstance(uuid_str, str):
            uuid_str = (
                uuid_str.strip().lower()
            )  # Strip whitespace and convert to lowercase
            try:
                val = uuid.UUID(uuid_str, version=4)
                # Ensure the formatted UUID matches the input
                return str(val) if str(val) == uuid_str else np.nan
            except ValueError:
                return np.nan
        return np.nan

    def run(self):
        """
        Runs all validation methods on the DataFrame.
        """
        self.validate_index("index")
        self.validate_name("first_name")
        self.validate_name("last_name")
        self.validate_date("date_of_birth")
        self.validate_company("company")
        self.validate_emails("email_address")
        self.validate_address("address")
        self.validate_country("country")
        self.validate_country_codes("country_code")
        self.validate_phone_number("phone_number")
        self.validate_date("join_date")
        self.validate_user_uuid("user_uuid")
        print(self.df.head())
        print("Before dropping the null value and invalid values:")
        print(self.df.info())
        print("After dropping the null value and invalid values:")
        self.df.dropna(inplace=True)





if __name__ == "__main__":
    country_code = {"Germany": "DE", "United Kingdom": "GB", "United States": "US"}
    # Load your data
    df = pd.read_csv(
        "/home/scala/Documents/aicore_project/multinational-retail-data-centralisation312/main/table.csv"
    )

    # Create an instance of DataCleaning
    dc = DataCleaning(df, country_code)
    dc.run()
    # Apply email validation
