# ✅ Problem Statement:

# 📝 Task:
# For each customer, find the first product they purchased (based on earliest purchase_date).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("FirstPurchasePerCustomer").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/first_purchases.csv")
df.show()

windowSpec = Window.partitionBy("customer_id").orderBy("purchase_date")

df_ranked = df.withColumn("rn", row_number().over(windowSpec))
df_result = df_ranked.filter(col("rn") == 1).drop("rn")
df_result.show()
