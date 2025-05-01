# ✅ Problem 5: Customers with No Orders

# 📄 Input Files:
# 	•	customers.csv → customer_id, name
# 	•	orders.csv → order_id, customer_id

# ⸻

# 🧩 Problem Statement:

# Find all customers who never placed an order.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, datediff, expr
from pyspark.sql.functions import sum as pyspark_sum
# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("RunningTotalOfSales").getOrCreate()

df_customers = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/Coding-Practice/Week1/May1/data/customers.csv")
df_customers.show()

df_orders = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/Coding-Practice/Week1/May1/data/orders.csv")
df_orders.show()

df_joined = df_customers.join(df_orders, on = "customer_id", how = "leftanti")
df_joined.show()