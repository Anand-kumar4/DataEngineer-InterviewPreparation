# ✅ Problem 5: Explode Multi-Item Transactions

# 📄 Dataset: multi_item_txns.csv

# 🎯 Goal:

# Flatten the items array so that each item appears in its own row, while retaining the associated txn_id.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, trim



# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("MultiItemTxns").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/multi_item_txns.csv")
df.show()

cleaned_items = regexp_replace(col("items"), r'["\[\]]', "")
df_with_array = df.withColumn("items_array", split(cleaned_items, ","))

df_exploded = df_with_array.select("txn_id", explode("items_array").alias("item"))
df_exploded = df_exploded.withColumn("item", trim(col("item")))
df_exploded.show()
