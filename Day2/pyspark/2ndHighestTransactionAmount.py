# ✅ Problem 1: Second Highest Transaction Amount per User

# 📄 Input File: transactions.csv
# 📌 Columns: user_id, amount, txn_date

# ⸻

# 🧩 Problem Statement:

# For each user, find the second highest transaction amount.


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,col

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("2ndHighestTransactionAmount").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day2/data/transactions.csv")
df.show()

windowSpec = Window.partitionBy("user_id").orderBy(col("amount").desc())

df_ranked = df.withColumn("rnk", dense_rank().over(windowSpec))
df_ranked.show()

df_filtered = df_ranked.filter(df_ranked["rnk"] == 2)
df_filtered.show()

