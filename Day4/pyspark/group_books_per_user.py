# ✅ Problem 1: Group Books per User

# 📄 Problem Statement:

# You are given a dataset of users and the books they’ve purchased. 
# Your goal is to return one row per user, where the books are aggregated into a comma-separated list. If a user hasn’t purchased any book (i.e., value is NULL), show NULL.

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, concat_ws, col

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("BooksPurchased").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/books_purchased.csv")
df.show()

df_grouped = df.groupBy("Name").agg(
    concat_ws(',', collect_list("Books_purchased")).alias("Books_purchased")
)
df_grouped.show()