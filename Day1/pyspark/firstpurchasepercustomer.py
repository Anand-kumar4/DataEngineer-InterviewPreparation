# ✅ Problem 1: First Purchase per Customer

# 📄 Input File: transactions.csv
# 📌 Columns: customer_id, txn_date, amount

# ⸻

# 🧩 Problem Statement:

# From the transactions data, find the first purchase (i.e. earliest txn_date) for each customer along with the amount.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("Top2SalaryPerDept").getOrCreate()

df = spark.read.option("header", "True").csv("/Users/madhukhanzode/Documents/Coding-Practice/Week1/May1/data/transactions.csv")
df.show()

windowSpec = Window.partitionBy("customer_id").orderBy(col("txn_date").asc())

df_ranked = df.withColumn("row_num", row_number().over(windowSpec))
df_ranked.show()

transactions_filtered = df_ranked.filter(df_ranked["row_num"] == 1)
transactions_filtered.show()