# ✅ Problem 6: Fill Missing Dates with Zero Revenue

# 📄 Dataset: revenue_with_gaps.csv

# 🎯 Goal:
# Create a row for every date from the minimum to the maximum sale_date,
# and set revenue = 0 for missing days.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, min, max, sequence, explode, lit, coalesce

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("FileMissingDates").getOrCreate()

# Step 1: Read the data with header
df = spark.read.option("header", "True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day5/data/revenue_with_gaps.csv")

# Step 2: Convert 'date' column to proper DateType and rename it to 'sale_date'
df = df.withColumn("sale_date", to_date(col("date"))).drop("date")
df.show()

# Step 3: Get the minimum and maximum dates from the dataset
date_range = df.select(min("sale_date").alias("min_date"), max("sale_date").alias("max_date")).collect()[0]
min_date = date_range["min_date"]
max_date = date_range["max_date"]

# Step 4: Create a DataFrame with a sequence of all dates between min and max
date_df = spark.createDataFrame([(1,)], ["dummy"])
date_df = date_df.select(explode(sequence(lit(min_date), lit(max_date))).alias("sale_date"))

# Step 5: Left join the full date range with the original data to find missing dates
joined_df = date_df.join(df, on="sale_date", how="left")

# Step 6: Replace null revenue values with 0 for the missing dates
final_df = joined_df.withColumn("revenue", coalesce(col("revenue"), lit(0)))
final_df.show()