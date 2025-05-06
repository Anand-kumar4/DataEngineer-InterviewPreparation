# Identify customers whose transaction amounts strictly increased on each subsequent transaction date.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lag, sum as spark_sum, count
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("IncreasingTransactionAmounts").getOrCreate()

# Sample transaction data
data = [
    (1, 101, "2024-04-10", 200),
    (2, 101, "2024-04-12", 300),
    (3, 101, "2024-04-13", 250),
    (4, 102, "2024-04-10", 100),
    (5, 102, "2024-04-11", 200),
    (6, 102, "2024-04-12", 300),
]

# Schema definition
schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("txn_date", StringType(), False),
    StructField("amount", IntegerType(), False)
])

# Create DataFrame
df_transactions = spark.createDataFrame(data, schema)
df_transactions.show()

windowSpec = Window.partitionBy("customer_id").orderBy(col("txn_date"))

df_transactions_1 = df_transactions.withColumn("previous_amount", lag("amount", 1).over(windowSpec))
df_transactions_1 = df_transactions_1.withColumn("is_increasing", col("amount") > col("previous_amount"))
df_transactions_1.show()


df_check = df_transactions_1.groupBy("customer_id").agg(
    spark_sum(col("is_increasing").cast("int")).alias("increasing_count"),
    count(col("is_increasing")).alias("total_comparisons")
)
df_check.show()

df_result = df_check.filter(col("increasing_count") == col("total_comparisons"))
df_result.select("customer_id").show()

