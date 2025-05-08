# ✅ Problem 3: Top Region by Average Revenue per Product

# 📄 Problem Statement:

# You’re given a dataset of product sales with three columns: region, product, and amount.
# Your task is to determine the region that has the highest average revenue for each product.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, avg
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("TopRegionByAvgRevenue").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day4/data/sales_region.csv")
df.show()



df_grouped = df.groupBy('product','region').agg(avg('amount').alias("avg_revenue"))
df_grouped.show()
windowSpec = Window.partitionBy('product').orderBy(col('avg_revenue').desc())

df_ranked = df_grouped.withColumn("rnk", dense_rank().over(windowSpec))
df_ranked.show()

df_top_region = df_ranked.filter(col("rnk") == 1).drop("rnk")
df_top_region.select("product", "region", "avg_revenue").show()