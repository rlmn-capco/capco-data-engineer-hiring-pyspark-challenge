# capco-data-engineer-hiring-pyspark-challenge
PySpark coding challenge for data engineering candidates.

## Environment:
- Spark Version: 3.5.0
- Python Version: 3.9.18

## Read-Only Files:
- `src/app.py`
- `src/tests/test_pipeline.py`
- `src/tests/test_helper.py`
- `src/main/__init__.py`
- `src/main/base/__init__.py`
- `src/main/job/__init__.py`
- `make.sh`
- `data/*`

## Requirements:
### The Utility Bank account master asset
#### Background
Our client, The Utility Bank (TUB), is an international bank providing financial services to small and medium-sized 
business around the world. A group of relationship managers, working for TUB, are finding it hard to see which accounts 
relate to their customers. They currently receive separate spreadsheets from the IT department that contain details 
of accounts and details of customers. 

The relationship managers have complained from a long time now about data quality issues and also the manual tasks they 
need to do to get the data into the format they want.

You have been tasked with writing a program to create the account master asset that meets their requirements, so they no 
longer need to chase the IT department for extracts nor do any manual transformations themselves.

#### Inputs
A data analyst in your team has found four inputs that you will need to use, they have provided the following information 
about the inputs:

1. **accounts.csv** - a large asset containing approximately 500 million account records. Data quality issues have been identified:
   1. the `account_name` field looks like it contains values that are entered manually as there are examples of the same account 
   name appearing but with different casing e.g. some lowercase, some mixed case etc... These values should be treated as the 
   same account name.
   2. there are duplicate records when there should not be.
   3. the `balance` field for some records is NULL, this does not make sense and again seems like a manual entry error.
2. **customers.csv** - an asset containing approximately 10 million customer records. New records are appended to this asset 
when a customer's details change, we only care about the latest record for a given customer. Data quality issues have been identified:
   1. the `country` field has values that sometimes have leading and/or trailing whitespace.
   2. the `local_currency` field has mixed case values, currency codes should all be uppercase.
3. **countries.csv** - a small reference asset that maps country codes to country names. There are no known data quality 
issues with this input, but there is no guarantee of completeness i.e. it may not include all countries.
4. **currencies.csv** - a small reference asset that maps currency codes to $USD exchange rates. There are no known data 
quality issues with this input, but there is no guarantee of completeness i.e. it may not include all currencies.

#### Tests
A tester in our team has reviewed the requirements and written up test cases for each function that you need to 
implement. These tests are all currently failing, you can find them in `src/tests/test_pipeline.py` and the helper 
methods in `src/tests/test_helper.py` that will help you understand the schemas of the inputs and expected outputs.

#### What you need to do
There are five functions that need to be implemented, these are:

1. `#init_spark_session` - create a SparkSession
2. `#process_accounts` - implement the logic to process the accounts input. Pay attention to the data quality issues identified by the data analyst.
3. `#process_customers` - implement the logic to process the customers input. Pay attention to the data quality issues identified by the data analyst. We are only interested in the latest record for a customer.
4. `#enrich_customers` - implement the logic to enrich the processed customers with `country_name` values and `rate` values.  If a country name cannot be found then use the constant "Unknown".
5. `#create_final_output` - relate the processed accounts to the enriched customers, if a customer cannot be found for an account maintain the account detail and leave the customer details as NULL. Create a new column called 
`balance_usd` that converts the balance in local currency to the $USD amount using the rate for that currency. Create a column called `next_balance` that has the balance for the same account on the next available `balance_date`, 
if there is no next balance date, use the record's `balance` value.

After implementing these functions, all tests should pass.

## Commands
- run: 
```bash
source venv/bin/activate; cd src; python3 app.py ../data/data_file1.csv ../data/data_file2.csv
```
- install: 
```bash
bash install.sh; source venv/bin/activate; pip3 install -r requirements.txt
```
- test: 
```bash
source venv/bin/activate; cd src; py.test -p no:warnings
```
