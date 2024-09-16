# Project Title: Sales Data Extraction and Validation

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
yourproject/
│
├── main/
│   ├── data_extraction.py      # Main script for data extraction and validation
│   ├── database_utils.py       # Database connection and operations
│   └── validation.py           # Pydantic models for data validation
│
├── data/                       # Folder to store raw data files
├── logs/                       # Logs of the application
├── tests/                      # Unit tests for the project
│
├── README.md                   # Project README file
├── requirements.txt            # Python dependencies
└── .env                        # Environment variables

```

## License
This project is licensed under the MIT License - see the LICENSE [file](https://opensource.org/license/mit) for details.