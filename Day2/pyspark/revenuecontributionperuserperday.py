# ✅ Problem 5: Revenue Contribution % Per Product Per Day

# 📄 Input File: daily_sales.csv
# 📌 Columns: sale_date, product_id, revenue

# ⸻

# 🧩 Problem Statement:

# For each day, calculate the percentage contribution of each product’s revenue to that day’s total revenue.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as pyspark_sum, round

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("RevenueContribution").getOrCreate()

df_daily_sales = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day2/data/daily_sales.csv")
df_daily_sales.show()

windowSpec = Window.partitionBy("sale_date")

df_daily_total = df_daily_sales.withColumn("daily_total", pyspark_sum("revenue").over(windowSpec))

df_result = df_daily_total.withColumn("revenue_pct", round(col("revenue") / col("daily_total") * 100, 2))
df_result.show()