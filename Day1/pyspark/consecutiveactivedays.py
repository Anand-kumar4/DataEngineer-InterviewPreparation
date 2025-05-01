# ✅ Problem 3: Consecutive Active Days

# 📄 Input File: user_activity.csv
# 📌 Columns: user_id, activity_date

# ⸻

# 🧩 Problem Statement:

# From the user_activity data, find users who were active for 3 or more consecutive days.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col, datediff, expr

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("ConsecutiveActiveDays").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/Coding-Practice/Week1/May1/data/user_activity.csv")
df.show()

windowSpec = Window.partitionBy("user_id").orderBy("activity_date")
df_numbered = df.withColumn("rn", row_number().over(windowSpec))
df_numbered.show()

# Step 2: Create a group key for consecutive days
df_grp = df_numbered.withColumn("grp", datediff(col("activity_date"), expr("'1970-01-01'")) - col("rn"))
df_grp.show()

df_streaks = df_grp.groupBy("user_id","grp").count()
df_streaks.show()

df_result = df_streaks.filter(col("count") >= 3).select("user_id").distinct()
df_result.show()
