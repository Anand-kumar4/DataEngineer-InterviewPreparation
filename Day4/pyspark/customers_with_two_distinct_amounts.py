# ✅ Problem 5: Customers with Exactly 2 Distinct Purchase Amounts

# 📄 Problem Statement:

# You’re given a dataset of customer transactions. Your task is to identify customers who have made exactly 2 distinct transaction amounts.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count_distinct



# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("CustomersWithTwoDistinctAmounts").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/distinct_amounts.csv")
df.show()

df_grouped = df.groupBy('customer_id').agg(count_distinct('amount').alias('cnt'))
df_grouped.show()

df_filtered = df_grouped.filter(col('cnt') == 2)
df_filtered.select("customer_id").show()

