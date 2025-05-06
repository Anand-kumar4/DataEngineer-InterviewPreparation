# ✅ Task:

# Write a PySpark program to return top 2 most recent orders for each customer based on order_date.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("Day3-Problem1").getOrCreate()

# Define schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True)
])

# Create DataFrame
df_orders = spark.createDataFrame([
    (1, 1, "2024-04-15"),
    (2, 1, "2024-04-17"),
    (3, 2, "2024-04-14"),
    (4, 2, "2024-04-16"),
    (5, 3, "2024-04-13"),
    (6, 3, "2024-04-12")
], schema)

df_orders.show()

windowSpec = Window.partitionBy("customer_id").orderBy(col("order_date").desc())

df_orders_ranked = df_orders.withColumn("row_num", row_number().over(windowSpec))

df_result = df_orders_ranked.filter(col("row_num") <= 2)
df_result.show()