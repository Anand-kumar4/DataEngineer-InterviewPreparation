# ✅ Problem 7: Product Category Popularity Trends

# 🎯 Goal:

# Track monthly sales per product category, and identify trend direction compared to the previous month.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format, sum as spark_sum, lag
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("ProductCategoryPopularityTrends").getOrCreate()

# 📥 Load the category_monthly_sales.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/category_monthly_sales.csv")
df.show()

# 🗓️ Convert sale_date from string to date format for accurate ordering
df = df.withColumn("sale_date", to_date(col("sale_date")))

df = df.select("category", "sale_date", "revenue", date_format("sale_date", "yyyy-MM").alias("month"))
df.show()

df_agg = df.groupBy("category", "month").agg(spark_sum("revenue").alias("total_revenue"))
df_agg.show()

windowSpec = Window.partitionBy("category").orderBy("month")
df_lag = df_agg.withColumn("previous_month_revenue", lag(col("total_revenue"), 1).over(windowSpec))

from pyspark.sql.functions import when

df_trend = df_lag.withColumn(
    "trend",
    when(col("previous_month_revenue").isNull(), "-")
    .when(col("total_revenue") > col("previous_month_revenue"), "UP")
    .when(col("total_revenue") < col("previous_month_revenue"), "DOWN")
    .otherwise("SAME")
)

df_trend.select("category", "month", "total_revenue", "trend").orderBy("category", "month").show(truncate=False)