# ✅ Problem 2: Highest Salary per Department

# 📄 Input File: employees.csv
# 📌 Columns: emp_id, name, department, salary

# ⸻

# 🧩 Problem Statement:

# From the employees table, find the employee(s) with the highest salary in each department.
# If multiple employees share the top salary in the same department, include them all.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,col

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("Top2SalaryPerDept").getOrCreate()

df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/Coding-Practice/Week1/May1/data/employees.csv")
df.show()

windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())

df_ranked = df.withColumn("rnk", dense_rank().over(windowSpec))
df_ranked.show()

df_result = df_ranked.filter(df_ranked["rnk"] == 1)
df_result.show()