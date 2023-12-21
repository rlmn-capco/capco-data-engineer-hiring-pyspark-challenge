from pyspark.sql import SparkSession, DataFrame
from main.base import PySparkJobInterface


class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        # TODO: Put your code here
        ...

    def process_accounts(self, accounts: DataFrame) -> DataFrame:
        # TODO: Put your code here
        ...

    def process_customers(self, customers: DataFrame) -> DataFrame:
        # TODO: Put your code here
        ...

    def enrich_customers(self, processed_customers: DataFrame, countries: DataFrame, currencies: DataFrame) -> DataFrame:
        # TODO: Put your code here
        ...

    def create_final_output(self, processed_accounts: DataFrame, enriched_customers: DataFrame) -> DataFrame:
        # TODO: Put your code here
        ...
