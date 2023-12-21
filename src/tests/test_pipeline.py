import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from main.job.pipeline import PySparkJob

from tests.test_helper import (
    get_account_input_schema,
    get_processed_account_schema,
    generate_account_input,
    generate_processed_account,
    get_customer_input_schema,
    generate_customer_input,
    get_currency_input_schema,
    generate_currency_input,
    get_country_input_schema,
    generate_country_input,
    get_date,
    generate_enriched_customer,
    get_enriched_customer_schema, get_output_schema, generate_output, get_usd
)

job = PySparkJob()


def create_sample(sample, data_schema):
    return job.spark.createDataFrame(data=sample, schema=data_schema)


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession), "-- spark session not implemented"


@pytest.mark.filterwarnings("ignore")
def test_account_processing():
    accounts = [
        generate_account_input(acc_num="GB001001", acc_name="Cupcake Co. Main", cust_id="GB001", bal=10.0,
                               bal_date=20230102.0),
        generate_account_input(acc_num="GB001001", acc_name="Cupcake Co. Main", cust_id="GB001", bal=10.0,
                               bal_date=20230102.0),
        generate_account_input(acc_num="GB001002", acc_name="CUPCAKE CO. DD", cust_id="GB001", bal=None,
                               bal_date=20230103.0),
        generate_account_input(acc_num="PL001001", acc_name="Babka Cake LTD", cust_id="PL001", bal=100.5,
                               bal_date=20230103.0),
    ]
    accounts_df = create_sample(accounts, get_account_input_schema())

    expected_processed_accounts = [
        generate_processed_account(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", cust_id="GB001", bal=10.0,
                                   bal_date=get_date("2023-01-02")),
        generate_processed_account(acc_num="PL001001", acc_name="BABKA CAKE LTD", cust_id="PL001", bal=100.5,
                                   bal_date=get_date("2023-01-03")),
    ]
    expected_df = (create_sample(expected_processed_accounts, get_processed_account_schema())
                   .orderBy("account_number", "account_name", "balance_date"))

    actual_df = job.process_accounts(accounts_df).orderBy("account_number", "account_name", "balance_date")

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, underline_cells=True)


@pytest.mark.filterwarnings("ignore")
def test_account_processing_dq():
    accounts = [
        generate_account_input(acc_num="GB001001", acc_name="Cupcake Co. Main", cust_id="GB001", bal=10.0,
                               bal_date=20230102.0),
        generate_account_input(acc_num="GB001001", acc_name="cupcake co. main", cust_id="GB001", bal=10.0,
                               bal_date=20230102.0),
        generate_account_input(acc_num="GB001001", acc_name="cupcake co. main", cust_id="GB001", bal=100.0,
                               bal_date=20230103.0),
        generate_account_input(acc_num="GB001002", acc_name="CUPCAKE CO. DD", cust_id="GB001", bal=None,
                               bal_date=20230103.0),
    ]
    accounts_df = create_sample(accounts, get_account_input_schema())

    expected_processed_accounts = [
        generate_processed_account(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", cust_id="GB001", bal=10.0,
                                   bal_date=get_date("2023-01-02")),
        generate_processed_account(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", cust_id="GB001", bal=100.0,
                                   bal_date=get_date("2023-01-03")),
    ]
    expected_df = (create_sample(expected_processed_accounts, get_processed_account_schema())
                   .orderBy("account_number", "account_name", "balance_date"))

    actual_df = job.process_accounts(accounts_df).orderBy("account_number", "account_name", "balance_date")

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, underline_cells=True)


@pytest.mark.filterwarnings("ignore")
def test_customer_processing():
    customers = [
        generate_customer_input(cust_id="GB001", cust_name="The Cupcake Company", country="GB", currency="EUR", last_updated=get_date("2023-05-16")),
        generate_customer_input(cust_id="GB001", cust_name="The Cupcake Company", country=" GB ", currency="GBP", last_updated=get_date("2023-05-17")),
        generate_customer_input(cust_id="PL001", cust_name="The Babka Company", country="PL", currency="pln", last_updated=get_date("2023-05-17")),
    ]
    customers_df = create_sample(customers, get_customer_input_schema())

    expected_processed_customers = [
        generate_customer_input(cust_id="GB001", cust_name="The Cupcake Company", country="GB", currency="GBP", last_updated=get_date("2023-05-17")),
        generate_customer_input(cust_id="PL001", cust_name="The Babka Company", country="PL", currency="PLN", last_updated=get_date("2023-05-17")),
    ]
    expected_df = (create_sample(expected_processed_customers, get_customer_input_schema())
                   .orderBy("customer_id", "customer_name", "last_updated"))

    actual_df = job.process_customers(customers_df).orderBy("customer_id", "customer_name", "last_updated")

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, underline_cells=True)


@pytest.mark.filterwarnings("ignore")
def test_enrich_customers():
    processed_customers = [
        generate_customer_input(cust_id="GB001", cust_name="The Cupcake Company", country="GB", currency="GBP", last_updated=get_date("2023-05-17")),
        generate_customer_input(cust_id="PL001", cust_name="The Babka Company", country="PL", currency="PLN", last_updated=get_date("2023-05-17")),
    ]
    processed_customers_df = create_sample(processed_customers, get_customer_input_schema())

    currencies = [
        generate_currency_input(currency_code="GBP", rate=1.14),
        generate_currency_input(currency_code="USD", rate=1.00),
        generate_currency_input(currency_code="EUR", rate=1.05),
    ]
    currency_df = create_sample(currencies, get_currency_input_schema())

    countries = [
        generate_country_input(country_code="GB", country_name="Great Britain"),
        generate_country_input(country_code="ZA", country_name="South Africa"),
    ]
    country_df = create_sample(countries, get_country_input_schema())

    expected_enriched_customers = [
        generate_enriched_customer(cust_id="GB001", cust_name="The Cupcake Company", country="GB", country_name="Great Britain", currency="GBP", rate=1.14),
        generate_enriched_customer(cust_id="PL001", cust_name="The Babka Company", country="PL", country_name="Unknown", currency="PLN", rate=None),
    ]
    expected_df = (create_sample(expected_enriched_customers, get_enriched_customer_schema())
                   .orderBy("customer_id", "customer_name", "country_name"))

    actual_df = (job.enrich_customers(processed_customers_df, country_df, currency_df)
                 .orderBy("customer_id", "customer_name", "country_name"))

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, underline_cells=True)


@pytest.mark.filterwarnings("ignore")
def test_create_final_output():
    processed_accounts = [
        generate_processed_account(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", cust_id="GB001", bal=10.0, bal_date=get_date("2023-01-02")),
        generate_processed_account(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", cust_id="GB001", bal=90.0, bal_date=get_date("2023-01-03")),
        generate_processed_account(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", cust_id="GB001", bal=-25.0, bal_date=get_date("2023-01-05")),
        generate_processed_account(acc_num="PL001001", acc_name="BABKA CAKE LTD", cust_id="PL001", bal=100.5, bal_date=get_date("2023-01-03")),
        generate_processed_account(acc_num="PL001001", acc_name="BABKA CAKE LTD", cust_id="PL001", bal=10.0, bal_date=get_date("2023-01-06")),
        generate_processed_account(acc_num="TH001001", acc_name="THAI KICK BOXING", cust_id="TH001", bal=1000.0, bal_date=get_date("2023-01-06")),
    ]
    processed_accounts_df = create_sample(processed_accounts, get_processed_account_schema())

    enriched_customers = [
        generate_enriched_customer(cust_id="GB001", cust_name="The Cupcake Company", country="GB", country_name="Great Britain", currency="GBP", rate=1.14),
        generate_enriched_customer(cust_id="PL001", cust_name="The Babka Company", country="PL", country_name="Unknown", currency="PLN", rate=None),
        generate_enriched_customer(cust_id="BM001", cust_name="The Bermuda Shorts Company", country="BM", country_name="Bermuda", currency="USD", rate=1.0),
    ]
    enriched_customers_df = create_sample(enriched_customers, get_enriched_customer_schema())

    expected_records = [
        generate_output(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", bal=10.0, bal_usd=get_usd(10.0, 1.14),
                        next_bal=90.0, bal_date=get_date("2023-01-02"), cust_id="GB001", cust_name="The Cupcake Company",
                        country="GB", country_name="Great Britain", currency="GBP"
                        ),
        generate_output(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", bal=90.0, bal_usd=get_usd(90.0, 1.14),
                        next_bal=-25.0, bal_date=get_date("2023-01-03"), cust_id="GB001", cust_name="The Cupcake Company",
                        country="GB", country_name="Great Britain", currency="GBP"
                        ),
        generate_output(acc_num="GB001001", acc_name="CUPCAKE CO. MAIN", bal=-25.0, bal_usd=get_usd(-25.0, 1.14),
                        next_bal=-25.0, bal_date=get_date("2023-01-05"), cust_id="GB001", cust_name="The Cupcake Company",
                        country="GB", country_name="Great Britain", currency="GBP"
                        ),
        generate_output(acc_num="PL001001", acc_name="BABKA CAKE LTD", bal=100.5, bal_usd=None, next_bal=10.0,
                        bal_date=get_date("2023-01-03"), cust_id="PL001", cust_name="The Babka Company", country="PL",
                        country_name="Unknown", currency="PLN"
                        ),
        generate_output(acc_num="PL001001", acc_name="BABKA CAKE LTD", bal=10.0,  bal_usd=None, next_bal=10.0,
                        bal_date=get_date("2023-01-06"), cust_id="PL001", cust_name="The Babka Company", country="PL",
                        country_name="Unknown", currency="PLN"
                        ),
        generate_output(acc_num="TH001001", acc_name="THAI KICK BOXING", bal=1000.0,  bal_usd=None, next_bal=1000.0,
                        bal_date=get_date("2023-01-06"), cust_id="TH001", cust_name=None, country=None,
                        country_name=None, currency=None
                        ),
    ]
    expected_df = (create_sample(expected_records, get_output_schema())
                   .orderBy("account_number", "customer_id", "balance_date"))

    actual_df = (job.create_final_output(processed_accounts_df, enriched_customers_df)
                 .orderBy("account_number", "customer_id", "balance_date"))

    assert_df_equality(actual_df, expected_df, ignore_column_order=True, underline_cells=True)
