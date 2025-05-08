# ✅ Problem 2: Find Users with All NULL Purchases

# 📄 Problem Statement:
# You’re given a dataset of users and the books they attempted to purchase.
# Your task is to find users for whom every entry in Books_purchased is NULL.

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("BooksPurchased").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/books_purchased_problem2.csv")
df.show()

df_cnt = df.groupBy("Name").agg(count("Books_purchased").alias("cnt"))
df_cnt.show()

df_result = df_cnt.filter(col('cnt') == 0).drop("cnt")
df_result.show()
