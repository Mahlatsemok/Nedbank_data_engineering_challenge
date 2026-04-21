from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # -----------------------------
    # Spark session
    # -----------------------------
    spark = SparkSession.builder \
        .appName("Gold Layer") \
        .getOrCreate()

    silver_path = "/data/output/silver"
    gold_path = "/data/output/gold"

    # -----------------------------
    # READ SILVER DATA
    # -----------------------------
    accounts_df = spark.read.format("delta").load(f"{silver_path}/accounts")
    customers_df = spark.read.format("delta").load(f"{silver_path}/customers")
    transactions_df = spark.read.format("delta").load(f"{silver_path}/transactions")

    # -----------------------------
    # DIM CUSTOMERS
    # -----------------------------
    dim_customers = customers_df.select(
        col("customer_id"),
        col("name"),
        col("email"),
        col("ingestion_timestamp")
    ).dropDuplicates()

    # -----------------------------
    # DIM ACCOUNTS
    # -----------------------------
    dim_accounts = accounts_df.select(
        col("account_id"),
        col("customer_id"),
        col("account_type"),
        col("balance"),
        col("ingestion_timestamp")
    ).dropDuplicates()

    # -----------------------------
    # FACT TRANSACTIONS
    # -----------------------------
    fact_transactions = transactions_df.select(
        col("transaction_id"),
        col("account_id"),
        col("amount"),
        col("transaction_type"),
        col("transaction_date"),
        col("ingestion_timestamp")
    ).dropDuplicates()

    # OPTIONAL: JOIN for enrichment (if needed by validation)
    fact_transactions = fact_transactions.join(
        dim_accounts.select("account_id", "customer_id"),
        on="account_id",
        how="left"
    )

    # -----------------------------
    # WRITE GOLD LAYER
    # -----------------------------
    dim_customers.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers")

    dim_accounts.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_accounts")

    fact_transactions.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_transactions")

    print("Gold layer completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()