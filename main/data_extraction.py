"""
data_extractor.py

This module contains the `DataExtractor` class, which is designed to extract data
from a relational database and load it into Pandas DataFrames. The class relies
on an instance of the `DatabaseConnector` class to handle database connections
and operations.

Classes
--------
- DataExtractor: A class for extracting data from a database into Pandas DataFrames.

Usage
------
1. Initialize a `DatabaseConnector` instance with proper database credentials.
2. Create a `DataExtractor` instance, passing the `DatabaseConnector` instance.
3. Use the `read_rds_table` method to extract data from a specific database table.

Example
-------
from database_utils import DatabaseConnector
from data_extractor import DataExtractor

# Initialize DatabaseConnector with appropriate credentials
db_connector = DatabaseConnector()
db_connector.init_db_engine()

# Initialize DataExtractor with the database connector
data_extractor = DataExtractor(connector=db_connector)

# Read data from a table named 'users'
df = data_extractor.read_rds_table('users')
print(df.head())
"""

import io
import json
import numpy as np
from io import StringIO
from typing import List, Optional, Dict
import time

import requests
import boto3
import pandas as pd
import jpype
import tabula
from sqlalchemy import (
    create_engine,
    text,
    update,
    func,
    cast,
    String,
    Date,
    Float,
    SmallInteger,
)

from main.database_utils import DatabaseConnector
from main.data_cleaning import (
    DataCleaning,
    UserModel,
    PaymentModel,
    StoreModel,
    ProductModel,
    OrderModel,
    DateModel,
)


class DataExtractor(DatabaseConnector, DataCleaning):
    """
    A class to extract and process data from various sources including databases, PDFs, APIs, and S3.
    It also includes data cleaning functionality inherited from DataCleaning.

    Inherits
    --------
    DatabaseConnector
        Provides database connection functionality.
    DataCleaning
        Provides data validation and cleaning functionality.
    """

    MAX_RETRIES = 3  # Maximum number of retries
    RETRY_DELAY = 2  # Delay between retries (in seconds)

    def __init__(
        self, model_class: Optional[object] = UserModel, class_name="data_cleaning"
    ) -> None:
        """
        Initializes DataExtractor by connecting to the database, setting up the S3 client,
        and initializing data cleaning functionality.

        Parameters
        ----------
        model_class : Optional[object], optional
            The model class to be used for data cleaning. Needed if using DataCleaning.
        class_name : str, optional
            The name of the class used for logging purposes in DataCleaning. Default is "data_cleaning".
        """
        # Initialize base classes
        DatabaseConnector.__init__(
            self, creds_path="db_creds.yaml", target_creds_path="target_db_creds.yaml"
        )
        DataCleaning.__init__(
            self, model_class=model_class, class_name=class_name
        )  # Pass model_class if needed

        # Initialize the S3 client
        self.s3 = boto3.client("s3")
        # Initialize the database engine
        self.init_db_engine()

    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        """
        Extracts a database table to a Pandas DataFrame.

        Parameters
        ----------
        table_name : str
            The name of the table to read from the database.

        Returns
        -------
        pd.DataFrame
            The extracted data as a DataFrame.

        Raises
        ------
        ValueError
            If the table name is not a string.
        RuntimeError
            If the database engine is not initialized or data extraction fails.
        """
        if not isinstance(table_name, str):
            raise ValueError("The table name must be a string.")

        if self.engine is None:
            raise RuntimeError("Database engine is not initialized.")

        try:
            query = f"SELECT * FROM {table_name}"
            with self.engine.connect() as connection:
                self.df = pd.read_sql(query, connection)
            return self.df
        except Exception as e:
            self.logger.error(f"Failed to read data from table '{table_name}': {e}")
            raise RuntimeError(
                f"Failed to read data from table '{table_name}': {e}"
            ) from e

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        """
        Extracts data from a PDF using Tabula.

        Parameters
        ----------
        link : str
            The URL or file path of the PDF.

        Returns
        -------
        pd.DataFrame
            The extracted data as a DataFrame.

        Raises
        ------
        RuntimeError
            If the PDF extraction fails.
        """
        if not jpype.isJVMStarted():
            jpype.startJVM()

        try:
            dfs = tabula.read_pdf(link, pages="all", multiple_tables=True)
            self.df = pd.concat(dfs, ignore_index=True)
            return self.df
        except Exception as e:
            self.logger.error(f"Failed to retrieve data from PDF: {e}")
            raise RuntimeError(f"Failed to retrieve data from PDF: {e}") from e

    def list_number_of_stores(self, endpoint: str, headers: Dict[str, str]) -> int:
        """
        Gets the number of stores from an API endpoint.

        Parameters
        ----------
        endpoint : str
            The API endpoint to get the number of stores.
        headers : Dict[str, str]
            The headers to include in the API request.

        Returns
        -------
        int
            The number of stores.

        Raises
        ------
        requests.RequestException
            If the API request fails.
        """
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                response = requests.get(endpoint, headers=headers)
                response.raise_for_status()
                return response.json().get("number_stores", 0)
            except requests.RequestException as e:
                self.logger.error(
                    f"Attempt {attempt} failed to retrieve the number of stores: {e}"
                )
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY)  # Wait before retrying
                else:
                    raise

    def retrieve_stores_data(
        self, endpoint: str, headers: Dict[str, str], num_stores: int
    ) -> pd.DataFrame:
        """
        Retrieves store data from an API.

        Parameters
        ----------
        endpoint : str
            The API endpoint template to retrieve store data.
        headers : Dict[str, str]
            The headers to include in the API request.
        num_stores : int
            The number of stores to retrieve data for.

        Returns
        -------
        pd.DataFrame
            DataFrame containing all store data.

        Raises
        ------
        requests.RequestException
            If the API request fails.
        """
        stores_df = []
        for store_index in range(0, num_stores):
            url = endpoint.format(store_number=store_index)
            for attempt in range(
                1, self.MAX_RETRIES + 1
            ):  # if failed, we allow to have another two attempts
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    stores_df.append(response.json())
                    break  # Exit retry loop if successful
                except requests.RequestException as e:
                    self.logger.error(
                        f"Attempt {attempt} failed for store number {store_index}: {e}"
                    )
                    if attempt < 3:
                        time.sleep(self.RETRY_DELAY2)  # Wait 2 secs before retrying
                    else:
                        self.logger.error(
                            f"Failed to retrieve data for store number {store_index} after 3 attempts."
                        )

        self.df = pd.DataFrame(stores_df)
        return self.df

    def extract_from_s3(self, s3_address: str) -> pd.DataFrame:
        """
        Extracts data from an S3 bucket and returns it as a DataFrame.

        Parameters
        ----------
        s3_address : str
            The S3 address in the format 's3://bucket_name/file_key'.

        Returns
        -------
        pd.DataFrame
            The extracted data as a DataFrame.

        Raises
        ------
        RuntimeError
            If data extraction from S3 fails.
        """
        bucket_name = s3_address.split("/")[2]
        file_key = "/".join(s3_address.split("/")[3:])
        try:
            obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
            data = obj["Body"].read()
            self.df = pd.read_csv(io.BytesIO(data))
            return self.df
        except Exception as e:
            self.logger.error(f"Failed to extract data from S3: {e}")
            raise RuntimeError(f"Failed to extract data from S3: {e}") from e

    def extract_json_from_S3(self, link):
        """
        Extracts JSON data from an S3 bucket and returns it as a DataFrame.

        Parameters
        ----------
        link : str
            The URL to the JSON file in the S3 bucket.

        Returns
        -------
        pd.DataFrame
            The extracted JSON data as a DataFrame.

        Raises
        ------
        requests.RequestException
            If the request to retrieve the JSON data fails.
        """
        r = requests.get(url=link)

        if r.ok:
            output = [r.json()]
        else:
            output = [{"error": r.content}]

        self.df = pd.DataFrame(output[0])
        return self.df

    def process_data(
        self,
        model_class=UserModel,
        module_name="datacleaning",
        class_name="DataCleaning",
    ):
        """
        Validates and cleans the extracted data, then prints valid and invalid data.

        Parameters
        ----------
        model_class : Type[BaseModel], optional
            The Pydantic model class used for data validation and cleaning. Default is UserModel.
        module_name : str, optional
            The name of the module used for logging purposes in DataCleaning. Default is "datacleaning".
        class_name : str, optional
            The name of the class used for logging purposes in DataCleaning. Default is "DataCleaning".
        """
        self.validate_and_clean_data()
        self.valid_df = pd.DataFrame(self.get_valid_data())
        invalid_df = self.get_invalid_data()
        self.save_invalid_data_log()
        print("Valid data:")
        print(self.valid_df.head())
        print("\nInvalid data:")
        print(invalid_df)


if __name__ == "__main__":

    # 1. Extracting, cleaning, and validating data
    # 1.1 Initialize DataExtractor
    de = DataExtractor()

    # 1.2 Extract User Data from AWS RDS database
    # Assign model class
    de.model_class = UserModel
    # Extract user data
    de.read_rds_table("legacy_users")
    print(de.df.info)
    # Process User data
    de.process_data()
    # load User data to postgresql database
    de.upload_to_db(de.valid_data, "dim_users")

    de.target_creds = de.read_db_creds(de.target_creds_path)

    # 1.3 Extract and process card data
    # Assign model class
    de.model_class = PaymentModel
    link = de.target_creds["card_details_link"]
    # Extract payment card data from AWS S3 as pdf file
    de.retrieve_pdf_data(link)
    print(de.df.head())
    # Process card data
    de.process_data()
    # upload card data to postgresql database
    de.upload_to_db(de.valid_data, "dim_card_details")

    # 1.4 Extract store data by using API
    # Assign headers and API endpoints to extract data
    headers = de.target_creds["header"]
    number_of_stores_endpoint = de.target_creds["number_of_stores_endpoint"]
    store_details_endpoint = de.target_creds["store_details_endpoint"]
    number_of_stores = de.list_number_of_stores(number_of_stores_endpoint, headers)
    print("total store numbers is: ", number_of_stores)
    # Assign model class
    de.model_class = StoreModel
    # Extract data
    de.retrieve_stores_data(store_details_endpoint, headers, number_of_stores)
    print(de.df.info())
    print(de.df.head())
    de.process_data()
    de.upload_to_db(de.valid_data, "dim_store_details")

    de.target_creds = de.read_db_creds(de.target_creds_path)

    # 1.5 Extract CSV product data from AWS S3
    # presume AWS CLI installed and configured
    de.model_class = ProductModel
    de.extract_from_s3(de.target_creds["product_table_link"])
    de.df.drop(columns=["Unnamed: 0"], errors="ignore")
    print(de.df.info())
    de.process_data()
    de.upload_to_db(de.valid_data, "dim_products")

    # 1.6 Extract json date_data from AWS s3
    # Assign model class
    de.model_class = DateModel
    # Extract user data
    link = de.target_creds["date_model_link"]
    de.extract_json_from_S3(link)
    print(de.df.head())
    # Process date data
    de.process_data()
    # upload order data to postgresql database
    de.upload_to_db(de.valid_data, "dim_date_times")

    # 1.7 Extract CSV table from AWS S3.
    de.model_class = OrderModel
    de.init_db_engine()
    de.df = de.read_rds_table("orders_table")
    columns_to_remove = ["first_name", "last_name", "1"]
    de.df.drop(columns=columns_to_remove, errors="ignore")
    print(de.df.head())
    # Process order data
    de.process_data()
    # upload order data to postgresql database
    de.upload_to_db(de.valid_data, "orders_table")
