# 🔄 Problem 7 : Churn Detection – Customers with No Purchase in Last 30 Days

# Dataset: churn_check.csv

# 🎯 Goal:

# Find customers who did not make any purchase in the last 30 days from a reference date (e.g., 2024-04-10).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, to_date, lit, datediff

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("ChurnedCustomers").getOrCreate()

# 📥 Load the churn_check.csv dataset and convert purchase_date to DateType
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/churn_check.csv")
df = df.withColumn("purchase_date", to_date(col("purchase_date")))
df.show()

# 🕒 Find the most recent purchase date per customer
latest_df = df.groupBy("customer_id").agg(spark_max("purchase_date").alias("last_purchase"))

# 📆 Define the reference date to compare against
reference_date = lit("2024-04-10")

# 🔢 Calculate the number of days since the last purchase
df_with_diff = latest_df.withColumn("days_since_last", datediff(reference_date, col("last_purchase")))

# 🚨 Identify customers who haven't purchased in the last 30 days
churned_customers = df_with_diff.filter(col("days_since_last") > 30)
churned_customers.select("customer_id").show()
