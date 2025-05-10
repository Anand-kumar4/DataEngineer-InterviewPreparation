# ✅ Problem 4: Category-wise Revenue Contribution %

# 🎯 Goal:

# For each category, compute the revenue contribution percentage of each product within that category.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("CategoryRevenuePercentage").getOrCreate()

# 📥 Load the category_sales.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/category_sales.csv")
df.show()

df_agg = df.groupBy("category").agg(spark_sum("revenue").alias("category_total"))
df_agg.show()

df_joined = df.join(df_agg, on = "category", how="inner")
df_joined.show()

df_result = df_joined.withColumn("contribution_pct", round(col("revenue") * 100 / col("category_total")))

df_result.show()