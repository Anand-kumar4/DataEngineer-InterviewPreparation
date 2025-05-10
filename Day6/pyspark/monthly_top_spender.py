# 💡 Problem:

# Find the customer(s) with the highest total monthly spend.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum as spark_sum, dense_rank, desc
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("MonthlyTopSpender").getOrCreate()

# 📥 Load the monthly_transactions.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/monthly_transactions.csv")
df.show()


# 🗓️ Extract 'month' in yyyy-MM format for grouping
df_with_month = df.select("customer_id","txn_date","amount", date_format("txn_date", "yyyy-MM").alias("month"))
df_with_month.show()


# 💰 Calculate total monthly spend per customer
df_agg = df_with_month.groupBy("customer_id","month").agg(spark_sum("amount").alias("total_monthly_spend"))
df_agg.show()


# 🏆 Define window to rank customers by monthly spend within each month
windowSpec = Window.partitionBy("month").orderBy(desc("total_monthly_spend"))


# 🥇 Apply dense_rank to get top spender(s) per month
df_ranked = df_agg.withColumn("dn", dense_rank().over(windowSpec))
df_ranked.show()


# 🎯 Filter to return only top spender(s) in each month
df_result = df_ranked.filter(col("dn") == 1)
df_result.select("customer_id", "month", "total_monthly_spend").show()