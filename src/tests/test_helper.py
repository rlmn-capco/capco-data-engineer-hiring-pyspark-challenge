import datetime

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType


def get_date(date_str: str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%d")


def get_usd(bal, rate):
    return bal/rate


def get_account_input_schema() -> StructType:
    return StructType([
        StructField("account_number", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("balance_date", DoubleType(), True),
    ])


def generate_account_input(
        acc_num="GB001001",
        acc_name="Cupcake Main Acc",
        cust_id="GB001",
        bal=100.0,
        bal_date=20230101.0,
) -> tuple:
    return acc_num, acc_name, cust_id, bal, bal_date


def get_processed_account_schema() -> StructType:
    return StructType([
        StructField("account_number", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("balance_date", DateType(), True),
    ])


def generate_processed_account(
        acc_num="GB001001",
        acc_name="CUPCAKE MAIN ACC",
        cust_id="GB001",
        bal=100.0,
        bal_date=get_date("2023-01-01"),
) -> tuple:
    return acc_num, acc_name, cust_id, bal, bal_date


def get_customer_input_schema() -> StructType:
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("local_currency", StringType(), True),
        StructField("last_updated", DateType(), True),
    ])


def generate_customer_input(
        cust_id="GB001",
        cust_name="The Cupcake Company",
        country="GB",
        currency="GBP",
        last_updated=get_date("2023-04-30"),
) -> tuple:
    return cust_id, cust_name, country, currency, last_updated


def get_enriched_customer_schema() -> StructType:
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("local_currency", StringType(), True),
        StructField("rate", DoubleType(), True),
    ])


def generate_enriched_customer(
        cust_id="GB001",
        cust_name="The Cupcake Company",
        country="GB",
        country_name="Great Britain",
        currency="GBP",
        rate=1.14,
) -> tuple:
    return cust_id, cust_name, country, country_name, currency, rate


def get_country_input_schema() -> StructType:
    return StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
    ])


def generate_country_input(
        country_code="GB",
        country_name="Great Britain",
) -> tuple:
    return country_code, country_name


def get_currency_input_schema() -> StructType:
    return StructType([
        StructField("currency_code", StringType(), True),
        StructField("rate", DoubleType(), True),
    ])


def generate_currency_input(
        currency_code="GBP",
        rate=1.14,
) -> tuple:
    return currency_code, rate


def get_output_schema() -> StructType:
    return StructType([
        StructField("account_number", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("balance_usd", DoubleType(), True),
        StructField("next_balance", DoubleType(), True),
        StructField("balance_date", DateType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("local_currency", StringType(), True),
    ])


def generate_output(
        acc_num="GB001001",
        acc_name="CUPCAKE MAIN ACC",
        bal=100.0,
        bal_usd=114.0,
        next_bal=100.0,
        bal_date=get_date("2023-01-01"),
        cust_id="GB001",
        cust_name="The Cupcake Company",
        country="GB",
        country_name="Great Britain",
        currency="GBP",
) -> tuple:
    return acc_num, acc_name, bal, bal_usd, next_bal, bal_date, cust_id, cust_name, country, country_name, currency
