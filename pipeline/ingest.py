from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def main():
    spark = SparkSession.builder \
        .appName("Bronze Ingestion") \
        .getOrCreate()

    # Read Input files
    accounts_df = spark.read.csv("/data/input/accounts.csv", header=True)
    customers_df = spark.read.csv("/data/input/customers.csv", header=True)
    transactions_df = spark.read.json("/data/input/transactions.jsonl")

    # Add ingestion timestamp
    accounts_df = accounts_df.withColumn("ingestion_timestamp", current_timestamp())
    customers_df = customers_df.withColumn("ingestion_timestamp", current_timestamp())
    transactions_df = transactions_df.withColumn("ingestion_timestamp", current_timestamp())

    # Write Bronze Delta tables
    accounts_df.write.format("delta").mode("overwrite").save("/data/output/bronze/accounts")
    customers_df.write.format("delta").mode("overwrite").save("/data/output/bronze/customers")
    transactions_df.write.format("delta").mode("overwrite").save("/data/output/bronze/transactions")

    spark.stop()

if __name__ == "__main__":
    main()