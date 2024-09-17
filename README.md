# Project Title: Sales Data Extraction and Validation
![Python](https://img.shields.io/badge/python-3.8-blue.svg)
![Pydantic](https://img.shields.io/badge/pydantic-v2-orange)
![AWS S3](https://img.shields.io/badge/AWS-S3-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Python CI](https://github.com/Michaelaicore/multinational-retail-data-centralisation312/actions/workflows/CI.yml/badge.svg)
![AWS S3](https://img.shields.io/badge/AWS-S3-green)
![API](https://img.shields.io/badge/API-Restful-blue)
![Unittest](https://img.shields.io/badge/unittest-passing-brightgreen)
![Logging](https://img.shields.io/badge/Logging-Enabled-blue)
![Pandas](https://img.shields.io/badge/Pandas-v1.3.3-orange)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-v1.4.23-red)
![YAML](https://img.shields.io/badge/YAML-Supported-yellow)
![AWS](https://img.shields.io/badge/AWS-Cloud-orange)
![S3](https://img.shields.io/badge/S3-Bucket-green)
![Boto3](https://img.shields.io/badge/Boto3-v1.18.26-blue)
![SQL](https://img.shields.io/badge/SQL-PostgreSQL-lightgrey)

## Table of Contents
1. [Project Description](#project-description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Project Description
This project is designed to extract sales data from various sources (including CSV files, API endpoint and JSON files stored on S3), clean the data, validate it using Pydantic models, and store it in a PostgreSQL database. The aim of this project is to streamline data handling processes and ensure data integrity through robust validation mechanisms.

### Key Features:
- Data extraction from multiple sources
- Data validation using Pydantic
- Data storage in PostgreSQL
- AWS S3 integration for file storage

### Technology involved:
- Handling large datasets in Python
- Using Pydantic for data validation
- Working with AWS S3 for file storage
- Database operations with PostgreSQL
- Extract data from API endpoint

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Michaelaicore/multinational-retail-data-centralisation312.git
   cd multinational-retail-data-centralisation312

2. **Create and activate your Python environment.** If you are using a virtual environment, activate it by running:

    ```bash
    python3 -m venv env
    source env/bin/activate
    ```
3. **Ensure you have all necessary dependencies installed.** If you haven't already set up your environment, you may need to install dependencies using `pip`:

    ```bash
    pip install -r requirements.txt
    ```
4. **Set up your AWS credentials in ~/.aws/credentials.** 

    4.1 ***Install AWS CLI*** If you haven't instal AWS CLI, go to [link](https://aws.amazon.com/cli/), follow the instruction to install AWS CLI regarding to your system. 

    4.2 ***Open AWS account, and get access key pair*** Sign up a AWS account, follow the [instruction](https://repost.aws/knowledge-center/create-access-key) to get your access key.

    4.3 ***Configure AWS CLI*** run following code, use access key pair complete the setup [process](https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html#cli-configure-files-methods)

        ```bash
        aws configure
        ```
5. **Install and configure your PostgreSQL database settings in the environment variables.**

## Usage 

1. Run the data extraction script:
    
    ```bash
    python main/data_extraction.py
    ```
    You can customize the extraction and validation process by modifying the scripts in the main folder.


## File Structure

```bash
multinational-retail-data-centralisation312/
├── db_creds.yaml
├── __init__.py
├── logging
│   ├── DateModel
│   │   ├── data_cleaning_errors.log
│   │   └── data_cleaning_invalid_data.csv
│   ├── OrderModel
│   │   └── data_cleaning_errors.log
│   ├── PaymentModel
│   │   ├── data_cleaning_errors.log
│   │   └── data_cleaning_invalid_data.csv
│   ├── ProductModel
│   │   ├── data_cleaning_errors.log
│   │   └── data_cleaning_invalid_data.csv
│   ├── StoreModel
│   │   ├── data_cleaning_errors.log
│   │   └── data_cleaning_invalid_data.csv
│   └── UserModel
│       ├── data_cleaning_errors.log
│       └── data_cleaning_invalid_data.csv
├── main
│   ├── database_utils.py
│   ├── data_cleaning.py
│   ├── data_extraction.py
│   └── __init__.py
│ 
├── README.md
├── requirements.txt
├── target_db_creds.yaml
└── test
    ├── __init__.py
    ├── test_database_utils.py
    ├── test_data_cleaning.py
    └── test_data_extraction.py
```

## License
This project is licensed under the MIT License - see the LICENSE [file](https://opensource.org/license/mit) for details.