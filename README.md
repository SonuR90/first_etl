## ETL Pipeline - S3 CSV File Validator

This project is an ETL pipeline that validates incoming CSV files from an S3 bucket, segregates valid/invalid data, and stores them in appropriate S3 folders. 
It is optimized for maintainability and clarity.


## Features

- Reads incoming `.csv` files from a date-based folder on S3
- Validates fields such as:
  - `product_id` against `product_master.csv`
  - Quantity * Price = Sales amount
  - Correct order date format and not a future date
  - Valid cities: Mumbai, Bangalore
- Uploads valid files to `success-files/YYYYMMDD/`
- Uploads invalid files to `rejected-files/YYYYMMDD/`
- Writes a rejection reason in an additional column
- Modular structure using Python best practices


## File Breakdown

| File             |                Purpose                         |
|------------------|------------------------------------------------|
| main.py          | Main controller script to run validation logic |
| s3_handler.py    | Handles all S3 operations                      |
| validator.py     | Performs row-level data validation             |
| email_utility.py | (Optional) Send email notifications using Gmail |
| logger.py        | (Optional) Centralized logger                  |
| config.ini       | Stores AWS and email credentials               |
| README.md        | Project documentation                          |


## Prerequisites

- Python 3.x
- Boto3 library
```bash
pip install boto3
