# ✅ Problem 2: Order-to-Delivery Duration

# 🎯 Goal:

# For each order, calculate the number of days it took to deliver the order.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_date
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("Order-To-Delivery").getOrCreate()

# 📥 Load the orders_delivery.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/orders_delivery.csv")
df.show()

# ⏱️ Calculate the difference in days between delivery_date and order_date
df = df.withColumn("duration_days", datediff(to_date("delivery_date"), to_date("order_date")))

# 📊 Show only the relevant columns: order_id, customer_id, and calculated duration
df.select("order_id", "customer_id", "duration_days").show()

