# 🧩 Problem 5: Top-Selling Product Per Region

# Objective:
# For each region, identify the product with the highest total sales.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("TopProductByRegion").getOrCreate()

data = [
    (1, "East", "iPhone", 500),
    (2, "East", "iPhone", 600),
    (3, "East", "iPad", 300),
    (4, "West", "MacBook", 1000),
    (5, "West", "iPad", 400),
    (6, "North", "iPhone", 450),
    (7, "North", "iPhone", 550),
    (8, "North", "MacBook", 300),
]

schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("product", StringType(), False),
    StructField("amount", IntegerType(), False),
])

df_sales = spark.createDataFrame(data, schema)
df_sales.show()

df_grouped = df_sales.groupBy("region", "product").agg(F.sum("amount").alias("total_sales"))

windowSpec = Window.partitionBy("region").orderBy(F.col("total_sales").desc())
df_ranked = df_grouped.withColumn("rnk", F.dense_rank().over(windowSpec))
df_result = df_ranked.filter(F.col("rnk") == 1)
df_result.show()