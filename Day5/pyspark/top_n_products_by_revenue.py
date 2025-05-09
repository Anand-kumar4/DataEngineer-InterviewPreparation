# ✅ Problem 4: Top 2 Products by Total Revenue

# 📄 Dataset: top_products.csv

# 🎯 Goal:

# Find the top 2 products with the highest total revenue across all transactions.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, dense_rank
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("TopNproductsByRevenue").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/top_products.csv")
df.show()

df_tot_rev = df.groupBy("product_id").agg(sum('revenue').alias('total_revenue'))
df_tot_rev.show()

windowSpec = Window.orderBy(col("total_revenue").desc())
df_ranked = df_tot_rev.withColumn("rnk", dense_rank().over(windowSpec))
df_ranked.show()

df_result = df_ranked.filter(col("rnk") <= 2)
df_result.select('product_id','total_revenue').show()