# 🎯 Goal:

# For each customer, identify the first product they purchased, and join this info back to the original orders table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, row_number, asc, to_date
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("FirstProductPurchased").getOrCreate()

# 📥 Load the orders_with_products.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/orders_with_products.csv")
df.show()

# 🗓️ Convert order_date from string to date format for accurate ordering
df = df.withColumn("order_date", to_date(col("order_date")))

# 🧮 Define window to rank products by order date per customer
windowSpec = Window.partitionBy("customer_id").orderBy(asc("order_date"))

# 🏅 Assign row number to each product per customer based on order date
df_ranked = df.withColumn("row_num", row_number().over(windowSpec))
df_ranked.show()

# 🥇 Filter to get the first product purchased per customer
df_first_order = df_ranked.filter(col("row_num") == 1)

# 🏷️ Rename 'product' column to 'first_product' for clarity
df_first_order = df_first_order.withColumnRenamed("product", "first_product")
df_first_order.select("customer_id", "first_product").show()

# 🔗 Join original data with first product info for each customer
df_joined = df.join(df_first_order.select("customer_id", "first_product"), on="customer_id", how="left")
df_joined.show()