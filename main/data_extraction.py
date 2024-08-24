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
import requests
import boto3
import pandas as pd
from io import StringIO
import pandas as pd
import numpy as np
import regex as reg  # Use regex module for Unicode properties
import uuid
from typing import Optional
import pandas as pd
import tabula

from database_utils import DatabaseConnector
from data_cleaning import DataCleaning


class DataExtractor(DatabaseConnector, DataCleaning):
    def __init__(self, df: pd.DataFrame = None) -> None:
        # Initialize the DatabaseConnector part
        DatabaseConnector.__init__(self)
        # Initialize the DataCleaning part with a placeholder DataFrame
        DataCleaning.__init__(
            self,
            df,
            country_code={
                "Germany": "DE",
                "United Kingdom": "GB",
                "United States": "US",
            },
        )
        # Initialize the S3 client inside the constructor (fix for the AttributeError)
        self.s3 = boto3.client('s3')
        # Initialize the database engine
        self.init_db_engine()

        # Load data after engine is initialized
        if df is None:
            self.df = self.read_rds_table("legacy_users")

    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        """
        Extracts a database table to a Pandas DataFrame.

        Parameters
        ----------
        table_name : str
            The name of the table to extract from the database.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the data from the specified table.

        Raises
        ------
        ValueError
            If the table name is not a string or if the table does not exist in the database.
        RuntimeError
            If there is an issue connecting to the database or querying the table.
        """
        if not isinstance(table_name, str):
            raise ValueError("The table name must be a string.")

        if self.engine is None:
            raise RuntimeError("DatabaseConnector is not initialized.")

        try:
            query = f"SELECT * FROM {table_name}"
            with self.engine.connect() as connection:
                self.df = pd.read_sql(query, connection)
        except Exception as e:
            raise RuntimeError(
                f"Failed to read data from table '{table_name}': {e}"
            ) from e

        return self.df

    def retrieve_pdf_data(self, link: str) -> pd.DataFrame:
        # Use tabula to read the PDF and extract data from all pages
        dfs = tabula.read_pdf(link, pages="all", multiple_tables=True)

        # Concatenate all the data into a single DataFrame
        data_table = pd.concat(dfs, ignore_index=True)

        return data_table

    # def list_number_of_stores(self, number_of_stores_endpoint, headers):
    #     response = requests.get(number_of_stores_endpoint, headers=headers)
    #     if response.status_code == 200:
    #         return response.json().get('number_of_stores', 0)  # Assuming 'number_of_stores' key is in the response
    #     else:
    #         print(f"Failed to retrieve the number of stores. Status code: {response.status_code}")
    #         return 0

    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        response = requests.get(number_of_stores_endpoint, headers=headers)
        if response.status_code == 200:
            print("API call successful. Here is the response:")
            print(response.json())  # Print the entire JSON response
            # Correct the key to 'number_stores' instead of 'number_of_stores'
            return response.json().get('number_stores', 0)
        else:
            print(f"Failed to retrieve the number of stores. Status code: {response.status_code}")
            return 0

    # def list_number_of_stores(self, number_of_stores_endpoint, headers):
    #     response = requests.get(number_of_stores_endpoint, headers=headers)
    #     if response.status_code == 200:
    #         print("API call successful. Here is the response:")
    #         print(response.json())  # Print the entire JSON response
    #         return response.json().get('number_of_stores', 0)
    #     else:
    #         print(f"Failed to retrieve the number of stores. Status code: {response.status_code}")
    #         return 0

    def retrieve_stores_data(self, store_details_endpoint, headers, num_stores):
        stores_data = []

        for store_number in range(1, num_stores + 1):
            url = store_details_endpoint.format(store_number=store_number)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                stores_data.append(response.json())  # Assuming response is a JSON object with store details
            else:
                print(f"Failed to retrieve data for store number {store_number}. Status code: {response.status_code}")

        # Convert the list of store data to a DataFrame
        return pd.DataFrame(stores_data)    
    
    def extract_from_s3(self, s3_address):
        # Split the s3 address into bucket and key
        bucket_name = s3_address.split('/')[2]
        file_key = '/'.join(s3_address.split('/')[3:])
        
        # Get the file object from S3
        obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
        
        # Read the content of the file
        data = obj['Body'].read().decode('utf-8')
        
        # Convert the data into a pandas DataFrame
        df = pd.read_csv(StringIO(data))
        return df



if __name__ == "__main__":
    country_code = {"Germany": "DE", "United Kingdom": "GB", "United States": "US"}
    de = DataExtractor()
    # df = de.read_rds_table("legacy_users")
    # de.df = df
    # de.df.info()
    # de.run()
    # de.df.info()
    # de.upload_to_db(de.df, "dim_users")
    # link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    # table = de.retrieve_pdf_data(link)
    # print(table.head())
    # de.df = table
    # cleaned_table = de.clean_card_data()
    # de.upload_to_db(de.df, "dim_card_details")

    # # Initialize the API key and endpoints
    # store_number = 3
    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_details_endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"

    number_of_stores = de.list_number_of_stores(number_of_stores_endpoint, headers)
    print("total store numbers is: ", number_of_stores)
    # Iterate over the range of store numbers

    all_stores_data = []

    # Loop through store numbers
    for store_number in range(1, number_of_stores_endpoint):
        url = store_details_endpoint.format(store_number=store_number)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            store_data = response.json()  # Convert the response to a dictionary
            all_stores_data.append(store_data)  # Add the store data to the list
        else:
            print(f"Failed to retrieve data for store number {store_number}. Status code: {response.status_code}")
    print(all_stores_data)
    # Convert the list of dictionaries to a DataFrame
    stores_df = pd.DataFrame(all_stores_data)
    print(stores_df)
    df = de.extract_from_s3("s3://data-handling-public/products.csv")
    print(df)