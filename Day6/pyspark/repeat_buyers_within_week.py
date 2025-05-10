# ✅ Problem 5: Detect Repeat Buyers Within a Week

# 🎯 Goal:

# Identify customers who made two or more purchases within a 7-day window (i.e., repeat buyers in a short span).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, datediff, to_date
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("RepeatBuyersWithinWeek").getOrCreate()

# 📥 Load the repeat_buyers.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/repeat_buyers.csv")
df.show()

windowSpec = Window.partitionBy("customer_id").orderBy("txn_date")

df_with_lag = df.withColumn("previous_txn_date", lag(col("txn_date"), 1).over(windowSpec))
df_with_lag.show()

df_date_diff = df_with_lag.withColumn("gap_between_txns", datediff(to_date("txn_date"), to_date("previous_txn_date")))
df_date_diff.show()

df_filtered = df_date_diff.filter(col("gap_between_txns") <= 7)
df_filtered.select("customer_id").distinct().show()