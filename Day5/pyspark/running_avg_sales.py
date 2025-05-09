# ✅ Problem 2: Running Average of Sales per Product

# 📄 Dataset: sales_avg.csv

# 🎯 Goal:

# For each product, compute the running average of revenue ordered by sale_date.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window


# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("RunningAvgSales").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/sales_avg.csv")
df.show()

windowSpec = Window.partitionBy("product_id").orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_avg = df.withColumn("running_avg", avg("revenue").over(windowSpec))
df_avg.show()