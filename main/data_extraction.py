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
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            return response.json().get("number_stores", 0)
        except requests.RequestException as e:
            self.logger.error(f"Failed to retrieve the number of stores: {e}")
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
        for store_index in range(1, num_stores + 1):
            url = endpoint.format(store_number=store_index)
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                stores_df.append(response.json())
                # self.df = pd.DataFrame(stores_df)
            except requests.RequestException as e:
                self.logger.error(
                    f"Failed to retrieve data for store number {store_index}: {e}"
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
    # upload User data to postgresql database
    de.upload_to_db(de.valid_data, "dim_users")

    # 1.3 Extract and process card data
    # Assign model class
    de.model_class = PaymentModel
    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    # Extract payment card data from AWS S3 as pdf file
    de.retrieve_pdf_data(link)
    print(de.df.head())
    # Process User data
    de.process_data()
    # upload card data to postgresql database
    de.upload_to_db(de.valid_data, "dim_card_details")

    # 1.4 Extract store data by using API
    # Assign headers and API endpoints to extract data
    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_of_stores_endpoint = (
        "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    )
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
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

    # 1.5 Extract CSV product data from AWS S3
    # presume AWS CLI installed and configured
    de.model_class = ProductModel
    de.extract_from_s3("s3://data-handling-public/products.csv")
    de.df.drop(columns=["Unnamed: 0"], errors="ignore")
    print(de.df.info())
    de.process_data()
    de.upload_to_db(de.valid_data, "dim_products")

    # 1.6 Extract json date_data from AWS s3
    # Assign model class
    de.model_class = DateModel
    # Extract user data
    link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    de.extract_json_from_S3(link)
    print(de.df.head())
    # Process User data
    de.process_data()
    # upload order data to postgresql database
    de.upload_to_db(de.valid_data, "dim_date_times")

    de.creds_path = "target_db_creds.yaml"
    de.read_db_creds(de.creds_path)
    de.init_db_engine()

    # Connect to the database

    # Start a transaction

    # Execute SQL commands to alter the schema

    # Change `date_uuid` and `user_uuid` to UUID
    # Assuming the connection to the database is already established

    # with de.engine.connect() as connection:
    #     connection.execute(text("BEGIN;"))

    #     # Your ALTER TABLE commands here

    #     connection.execute(text("COMMIT;"))

    # import logging
    # # Setup logging
    # logging.basicConfig()
    # logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    # try:
    #     with de.engine.connect() as connection:
    #         with connection.begin():
    #             # Check current column types
    #             result = connection.execute(text("""
    #                 SELECT column_name, data_type
    #                 FROM information_schema.columns
    #                 WHERE table_name = 'orders_table'
    #                 AND column_name IN ('user_uuid', 'date_uuid');
    #             """))

    #             current_schema = {}
    #             for row in result:
    #                 column_name = row[0]
    #                 data_type = row[1]
    #                 current_schema[column_name] = data_type

    #             # Define desired schema changes
    #             desired_schema = {
    #                 'user_uuid': 'UUID',
    #                 'date_uuid': 'UUID'
    #             }

    #             # Compare and apply schema changes if necessary
    #             alter_statements = []
    #             for column, desired_type in desired_schema.items():
    #                 current_type = current_schema.get(column)
    #                 if current_type != desired_type:
    #                     alter_statements.append(
    #                         f"ALTER COLUMN {column} SET DATA TYPE {desired_type} USING {column}::{desired_type}"
    #                     )

    #             if alter_statements:
    #                 connection.execute(text(f"""
    #                     ALTER TABLE orders_table
    #                     {', '.join(alter_statements)};
    #                 """))
    #                 print("Remaining schema updates applied and committed successfully.")
    #             else:
    #                 print("No additional schema changes needed.")

    # except Exception as e:
    #     print(f"An error occurred: {e}")

    # try:
    #     with de.engine.connect() as connection:
    #         with connection.begin():
    #             # Get the maximum length of country_code
    #             result = connection.execute(text("""
    #                 SELECT MAX(LENGTH(country_code)) AS max_country_code_length
    #                 FROM dim_users;
    #             """))
    #             max_length = result.fetchone()[0] or 0

    #             # Define desired schema changes
    #             alter_statements = [
    #                 "ALTER COLUMN first_name SET DATA TYPE VARCHAR(255)",
    #                 "ALTER COLUMN last_name SET DATA TYPE VARCHAR(255)",
    #                 "ALTER COLUMN date_of_birth SET DATA TYPE DATE USING date_of_birth::DATE",
    #                 f"ALTER COLUMN country_code SET DATA TYPE VARCHAR({max_length})",
    #                 "ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID",
    #                 "ALTER COLUMN join_date SET DATA TYPE DATE USING join_date::DATE"
    #             ]

    #             # Apply the schema changes
    #             connection.execute(text(f"""
    #                 ALTER TABLE dim_users
    #                 {', '.join(alter_statements)};
    #             """))
    #             print("Schema updates applied and committed successfully.")

    # except Exception as e:
    #     print(f"An error occurred: {e}")

    # def update_dim_store_details(engine):
    #     with engine.connect() as connection:
    #         # Replace 'latitude' and 'lat' with the correct column names
    #         merge_latitude_sql = """
    #         UPDATE dim_store_details
    #         SET latitude = COALESCE(latitude::FLOAT, lat::FLOAT);  -- Cast both columns to FLOAT
    #         """
    #         connection.execute(text(merge_latitude_sql))

    #         # Drop the redundant latitude column (replace with actual column name)
    #         drop_latitude_sql = """
    #         ALTER TABLE dim_store_details DROP COLUMN lat;  -- Replace with actual column name
    #         """
    #         connection.execute(text(drop_latitude_sql))

    #         # Change data types of the columns
    #         alter_table_sql = """
    #         ALTER TABLE dim_store_details
    #         ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::FLOAT,
    #         ALTER COLUMN locality SET DATA TYPE VARCHAR(255),
    #         ALTER COLUMN store_code SET DATA TYPE VARCHAR(20),  -- Replace with your max length
    #         ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT,
    #         ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::DATE,
    #         ALTER COLUMN store_type SET DATA TYPE VARCHAR(255),
    #         ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::FLOAT,
    #         ALTER COLUMN country_code SET DATA TYPE VARCHAR(5),  -- Replace with your max length
    #         ALTER COLUMN continent SET DATA TYPE VARCHAR(255);
    #         """
    #         connection.execute(text(alter_table_sql))

    #         # Update location column to set 'N/A' values to NULL
    #         update_location_sql = """
    #         UPDATE dim_store_details
    #         SET locality = NULL
    #         WHERE locality = 'N/A';
    #         """
    #         connection.execute(text(update_location_sql))

    #         # Commit changes
    #         connection.commit()

    #     print("Schema and data updates completed successfully.")

    # # Assuming `engine` is your SQLAlchemy engine
    # update_dim_store_details(de.engine)

    # def update_dim_products(engine):
    #     with engine.connect() as connection:
    #         # Step 1: Convert product_price to text, remove the £ character, then convert it back to numeric
    #         remove_currency_and_convert_sql = """
    #         -- Convert product_price to text, remove £ symbol, and convert back to numeric
    #         UPDATE dim_products
    #         SET product_price = CAST(REPLACE(CAST(product_price AS TEXT), '£', '') AS NUMERIC);
    #         """
    #         connection.execute(text(remove_currency_and_convert_sql))

    #         # Step 2: Add weight_class column if it doesn't exist
    #         add_weight_class_column_sql = """
    #         ALTER TABLE dim_products
    #         ADD COLUMN IF NOT EXISTS weight_class VARCHAR(20);
    #         """
    #         connection.execute(text(add_weight_class_column_sql))

    #         # Step 3: Update the weight_class column based on the weight range
    #         update_weight_class_sql = """
    #         UPDATE dim_products
    #         SET weight_class = CASE
    #             WHEN weight < 2 THEN 'Light'
    #             WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    #             WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    #             WHEN weight >= 140 THEN 'Truck_Required'
    #         END;
    #         """
    #         connection.execute(text(update_weight_class_sql))

    #         # Commit the changes
    #         connection.commit()

    # # Assuming you have an existing database engine instance
    # update_dim_products(de.engine)


# def update_dim_products(engine):
#     with de.engine.connect() as connection:
#         # Step 1: Rename the removed column to still_available
#         rename_column_sql = """
#         ALTER TABLE dim_products
#         RENAME COLUMN removed TO still_available;
#         """
#         connection.execute(text(rename_column_sql))

#         # Step 2: Convert columns to their required data types
#         # Convert product_price to FLOAT
#         convert_product_price_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;
#         """
#         connection.execute(text(convert_product_price_sql))

#         # Convert weight to FLOAT
#         convert_weight_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;
#         """
#         connection.execute(text(convert_weight_sql))

#         # Convert EAN to VARCHAR(13) (assuming 13 is the maximum length required)
#         convert_EAN_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN EAN TYPE VARCHAR(13);
#         """
#         connection.execute(text(convert_EAN_sql))

#         # Convert product_code to VARCHAR(11) (assuming 11 is the maximum length required)
#         convert_product_code_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN product_code TYPE VARCHAR(11);
#         """
#         connection.execute(text(convert_product_code_sql))

#         # Convert date_added to DATE
#         convert_date_added_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN date_added TYPE DATE USING date_added::DATE;
#         """
#         connection.execute(text(convert_date_added_sql))

#         # Convert uuid to UUID
#         convert_uuid_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN uuid TYPE UUID USING uuid::UUID;
#         """
#         connection.execute(text(convert_uuid_sql))

#         # Convert still_available to BOOL
#         convert_still_available_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL;
#         """
#         connection.execute(text(convert_still_available_sql))

#         # Convert weight_class to VARCHAR(20) (assuming 20 is the maximum length required)
#         convert_weight_class_sql = """
#         ALTER TABLE dim_products
#         ALTER COLUMN weight_class TYPE VARCHAR(20);
#         """
#         connection.execute(text(convert_weight_class_sql))

#         # Commit the changes
#         connection.commit()

    # # Assuming you have an existing database engine instance
    # update_dim_products(de.engine)

    # # def excute_sql(SQL):
    # #     connection.execute(text(sql))

    # def update_dim_store_details(engine):
    #     with engine.connect() as connection:
    #         # Replace 'latitude' and 'lat' with the correct column names
    #         merge_latitude_sql = """
    #         UPDATE dim_store_details
    #         SET latitude = COALESCE(latitude::FLOAT, lat::FLOAT);  -- Cast both columns to FLOAT
    #         """
    #         connection.execute(text(merge_latitude_sql))

    #         # Drop the redundant latitude column (replace with actual column name)
    #         drop_latitude_sql = """
    #         ALTER TABLE dim_store_details DROP COLUMN lat;  -- Replace with actual column name
    #         """
    #         connection.execute(text(drop_latitude_sql))

    #         # Change data types of the columns
    #         alter_table_sql = """
    #         ALTER TABLE dim_store_details
    #         ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::FLOAT,
    #         ALTER COLUMN locality SET DATA TYPE VARCHAR(255),
    #         ALTER COLUMN store_code SET DATA TYPE VARCHAR(20),  -- Replace with your max length
    #         ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT,
    #         ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::DATE,
    #         ALTER COLUMN store_type SET DATA TYPE VARCHAR(255),
    #         ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::FLOAT,
    #         ALTER COLUMN country_code SET DATA TYPE VARCHAR(5),  -- Replace with your max length
    #         ALTER COLUMN continent SET DATA TYPE VARCHAR(255);
    #         """
    #         connection.execute(text(alter_table_sql))

    #         # Update location column to set 'N/A' values to NULL
    #         update_location_sql = """
    #         UPDATE dim_store_details
    #         SET location = NULL
    #         WHERE location = 'N/A';
    #         """
    #         connection.execute(text(update_location_sql))

    #         # Commit changes
    #         connection.commit()

    #     print("Schema and data updates completed successfully.")

    # # Assuming `engine` is your SQLAlchemy engine
    # update_dim_store_details(de.engine)

    # def update_dim_store_details(engine):
    #     with engine.connect() as connection:
    #         # Merge latitude columns
    #         merge_latitude_sql = """
    #         UPDATE dim_store_details
    #         SET latitude = COALESCE(latitude, lat);
    #         """
    #         connection.execute(text(merge_latitude_sql))

    #         # Drop the redundant latitude column
    #         drop_latitude_sql = """
    #         ALTER TABLE dim_store_details DROP COLUMN latitude_column_2;
    #         """
    #         connection.execute(text(drop_latitude_sql))

    #         # Change data types of the columns
    #         alter_table_sql = """
    #         ALTER TABLE dim_store_details
    #         ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::FLOAT,
    #         ALTER COLUMN locality SET DATA TYPE VARCHAR(255),
    #         ALTER COLUMN store_code SET DATA TYPE VARCHAR(20), -- Replace with your max length
    #         ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT,
    #         ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_date::DATE,
    #         ALTER COLUMN store_type SET DATA TYPE VARCHAR(255),
    #         ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::FLOAT,
    #         ALTER COLUMN country_code SET DATA TYPE VARCHAR(5), -- Replace with your max length
    #         ALTER COLUMN continent SET DATA TYPE VARCHAR(255);
    #         """
    #         connection.execute(text(alter_table_sql))

    #         # Update location column to set 'N/A' values to NULL
    #         update_location_sql = """
    #         UPDATE dim_store_details
    #         SET location = NULL
    #         WHERE location = 'N/A';
    #         """
    #         connection.execute(text(update_location_sql))

    #         # Commit changes
    #         connection.commit()

    #     print("Schema and data updates completed successfully.")

    # # Assuming `engine` is your SQLAlchemy engine
    # update_dim_store_details(de.engine)
