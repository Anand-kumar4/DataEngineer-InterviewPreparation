# ✅ Day 4 – Problem 4: Flag High vs Low Value Transactions

# 📄 Problem Statement:

# You’re given a transaction dataset with txn_id, user_id, and amount.
# You need to classify each transaction as:
# 	•	"High" if the amount ≥ 300
# 	•	"Low" if the amount < 300

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when



# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("HighLowTransactions").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/transactions_flag.csv")
df.show()

df_result = df.withColumn(
        "value_category", when(df.amount < 300, 'Low').otherwise('High')
                          )
df_result.show()

