# ✅ Problem 3: First and Last Login Timestamp per User

# 📄 Input File: logins.csv
# 📌 Columns: user_id, login_time

# ⸻

# 🧩 Problem Statement:

# For each user, return their first login timestamp and last login timestamp.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, max

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("2ndHighestTransactionAmount").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day2/data/logins.csv")
df.show()

df_grouped = df.groupBy("user_id").agg(min("login_time").alias("first_login"), max("login_time").alias("last_login"))
df_grouped.show()

