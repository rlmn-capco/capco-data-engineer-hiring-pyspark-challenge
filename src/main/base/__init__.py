import abc
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType


class PySparkJobInterface(abc.ABC):

    def __init__(self):
        self.spark = self.init_spark_session()

    def read_csv(self, input_path: str) -> DataFrame:
        return self.spark.read.options(header=True, inferSchema=True).csv(input_path)

    def read_account_csv(self, input_path: str) -> DataFrame:
        return (self.read_csv(input_path)
                .withColumn("balance", col("balance").cast(DoubleType()))
                .withColumn("balance_date", col("balance_date").cast(DoubleType()))
                )

    def read_customer_csv(self, input_path: str) -> DataFrame:
        return self.read_csv(input_path).withColumn("last_updated", to_date(col("last_updated"), "yyyyMMdd"))

    def read_currency_csv(self, input_path: str) -> DataFrame:
        return self.read_csv(input_path).withColumn("rate", col("rate").cast(DoubleType()))

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        """Create spark session"""
        raise NotImplementedError

    @abc.abstractmethod
    def process_accounts(self, accounts: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def process_customers(self, customers: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def enrich_customers(self, processed_customers: DataFrame, countries: DataFrame, currencies: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abc.abstractmethod
    def create_final_output(self, processed_accounts: DataFrame, enriched_customers: DataFrame) -> DataFrame:
        raise NotImplementedError

    def stop(self) -> None:
        self.spark.stop()
