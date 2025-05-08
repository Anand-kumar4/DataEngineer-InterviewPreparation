# ✅ Problem 7: Customers Who Bought the Same Product More Than Once

# 📄 Problem Statement:

# You are given a purchase history. Identify all customers who have purchased the same product more than once (i.e., duplicate purchases of the same item).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count



# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("FirstPurchasePerCustomer").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/repeat_purchases.csv")
df.show()

df_grouped = df.groupBy("customer_id", "product").agg(count("*").alias("cnt"))
df_filtered = df_grouped.filter(col("cnt") > 1)
df_filtered.select("customer_id","product").show()

