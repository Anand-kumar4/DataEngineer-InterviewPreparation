# ✅ Problem 4: Products That Have Never Been Sold

# 📄 Input Files:
# 	•	products.csv → product_id, product_name
# 	•	sales.csv → sale_id, product_id, quantity, sale_date

# ⸻

# 🧩 Problem Statement:

# From the list of products, find the ones that have never been sold (i.e., their product_id doesn’t appear in sales).

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, min, max

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("2ndHighestTransactionAmount").getOrCreate()

df_products = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day2/data/products.csv")
df_products.show()

df_sales = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day2/data/sales.csv")
df_sales.show()

df_joined = df_products.join(df_sales, on="product_id", how="leftanti")
df_joined.show()
