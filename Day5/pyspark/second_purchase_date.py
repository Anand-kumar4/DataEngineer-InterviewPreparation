# ✅ Problem 1: Second Purchase Date per Customer

# 📄 Dataset: second_purchase.csv

# 🎯 Goal:

# For each customer, return the second purchase date (i.e., the second earliest transaction by purchase_date).


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("SecondPurchaseDate").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/second_purchase.csv")
df.show()

windowSpec = Window.partitionBy("customer_id").orderBy("purchase_date")

df_ranked = df.withColumn("rn", row_number().over(windowSpec))

df_result = df_ranked.filter(col("rn") == 2).drop("rn")
df_final = df_result.withColumnRenamed("purchase_date", "second_purchase_date")
df_final.show()