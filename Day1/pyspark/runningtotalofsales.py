# ✅ Problem 4: Running Total of Sales

# 📄 Input File: sales.csv
# 📌 Columns: date, product_id, revenue

# ⸻

# 🧩 Problem Statement:

# For each product, compute the running total of revenue over time ordered by date.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, expr
from pyspark.sql.functions import sum as pyspark_sum
# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("RunningTotalOfSales").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/Coding-Practice/Week1/May1/data/sales.csv")
df.show()

windowSpec = Window.partitionBy("product_id").orderBy("date").rowsBetween(Window.unboundedPreceding,Window.currentRow)
df_running_total_revenue = df.withColumn("cumulative_revenue", pyspark_sum(col("revenue")).over(windowSpec))
df_running_total_revenue.show()