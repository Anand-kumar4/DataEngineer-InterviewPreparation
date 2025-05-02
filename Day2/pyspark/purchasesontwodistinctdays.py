# ✅ Problem 2: Users with Purchases on Exactly 2 Distinct Days

# 📄 Input File: user_txns.csv
# 📌 Columns: user_id, txn_date

# ⸻

# 🧩 Problem Statement:

# Find users who made purchases on exactly 2 different days.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, count_distinct

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("2ndHighestTransactionAmount").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day2/data/user_txns.csv")
df.show()

df1 = df.groupBy("user_id").agg(count_distinct("txn_date").alias("count_distinct_transactions"))
df1.show()

df_filtered = df1.filter(df1["count_distinct_transactions"] == 2)
df_filtered.show()
