import sys

from pyspark.sql import DataFrame

from main.job.pipeline import PySparkJob


def main():
    job = PySparkJob()

    # Load input data to DataFrame
    print("<<Reading Inputs>>")
    accounts_input: DataFrame = job.read_account_csv(sys.argv[1])
    customers_input: DataFrame = job.read_customer_csv(sys.argv[2])
    countries_input: DataFrame = job.read_csv(sys.argv[3])
    currencies_input: DataFrame = job.read_currency_csv(sys.argv[4])

    print("<<Account Input>>")
    accounts_input.printSchema()
    accounts_input.show(100, False)
    print("<<Customer Input>>")
    customers_input.printSchema()
    customers_input.show(100, False)
    print("<<Country Input>>")
    countries_input.printSchema()
    countries_input.show(100, False)
    print("<<Currency Input>>")
    currencies_input.printSchema()
    currencies_input.show(100, False)

    # Implement account processing logic
    print("<<Processed Accounts>>")
    processed_accounts: DataFrame = job.process_accounts(accounts_input)
    processed_accounts.printSchema()
    processed_accounts.show(100, False)

    # Implement customer processing logic
    print("<<Processed Customers>>")
    processed_customers: DataFrame = job.process_customers(customers_input)
    processed_customers.printSchema()
    processed_customers.show(100, False)

    # Implement customer enrichment logic
    print("<<Enriched Customers>>")
    enriched_customers: DataFrame = job.enrich_customers(processed_customers, countries_input, currencies_input)
    enriched_customers.printSchema()
    enriched_customers.show(100, False)

    # Implement final output logic
    print("<<Final Output>>")
    final_output: DataFrame = job.create_final_output(processed_accounts, enriched_customers)
    final_output.printSchema()
    final_output.show(100, False)

    job.stop()


if __name__ == '__main__':
    main()
