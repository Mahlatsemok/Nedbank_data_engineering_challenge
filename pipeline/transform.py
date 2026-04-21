from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

def main():
    # -----------------------------
    # Spark Session (Docker ready)
    # -----------------------------
    spark = SparkSession.builder \
        .appName("Silver Layer") \
        .getOrCreate()

    # -----------------------------
    # INPUT (Bronze layer)
    # -----------------------------
    bronze_path = "/data/output/bronze"

    accounts_df = spark.read.format("delta").load(f"{bronze_path}/accounts")
    customers_df = spark.read.format("delta").load(f"{bronze_path}/customers")
    transactions_df = spark.read.format("delta").load(f"{bronze_path}/transactions")

    # -----------------------------
    # CLEAN ACCOUNTS
    # -----------------------------
    accounts_df = accounts_df.dropDuplicates()

    # standardise columns (safe cleaning)
    for c in accounts_df.columns:
        accounts_df = accounts_df.withColumn(c, trim(col(c)))

    # -----------------------------
    # CLEAN CUSTOMERS
    # -----------------------------
    customers_df = customers_df.dropDuplicates()

    for c in customers_df.columns:
        customers_df = customers_df.withColumn(c, trim(col(c)))

    # normalize email (example standardisation)
    if "email" in customers_df.columns:
        customers_df = customers_df.withColumn("email", lower(col("email")))

    # -----------------------------
    # CLEAN TRANSACTIONS
    # -----------------------------
    transactions_df = transactions_df.dropDuplicates()

    for c in transactions_df.columns:
        transactions_df = transactions_df.withColumn(c, trim(col(c)))

    # -----------------------------
    # JOIN ACCOUNTS + CUSTOMERS
    # -----------------------------
    if "customer_id" in accounts_df.columns and "customer_id" in customers_df.columns:
        accounts_df = accounts_df.join(
            customers_df,
            on="customer_id",
            how="left"
        )

    # -----------------------------
    # WRITE SILVER OUTPUT (Docker path)
    # -----------------------------
    silver_path = "/data/output/silver"

    accounts_df.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts")
    customers_df.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
    transactions_df.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")

    print("Silver layer completed successfully")

    spark.stop()


if __name__ == "__main__":
    main()