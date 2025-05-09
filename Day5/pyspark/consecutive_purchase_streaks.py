# ✅ Problem 3: Detect Customers with Consecutive Purchase Days

# 📄 Dataset: consecutive_purchases.csv

# 🎯 Goal:

# Find customers who made purchases on two or more consecutive calendar days (e.g., 2024-04-01 and 2024-04-02).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, datediff, to_date, lit, count
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("ConsecutivePurchaseStreaks").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/consecutive_purchases.csv")
df.show()

windowSpec = Window.partitionBy("customer_id").orderBy("purchase_date")

df_ranked = df.withColumn("rn", row_number().over(windowSpec))
df_ranked.show()

df_grp = df_ranked.withColumn(
    "date_diff",
    datediff(to_date(col("purchase_date")), to_date(lit("1970-01-01"))) - col("rn")
)
df_grp.show()

df_grouped = df_grp.groupBy("customer_id","date_diff").agg(count("*").alias("cnt"))
df_grouped.show()

df_result = df_grouped.filter(col("cnt") >= 2)
df_result.select("customer_id").distinct().show()