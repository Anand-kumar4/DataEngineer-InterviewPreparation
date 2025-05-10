# ✅ Problem 6: Sessionize Web Events

# 🎯 Goal:

# Group user events into sessions, where a session is defined as a gap of more than 30 minutes between events.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, when, sum as spark_sum, to_timestamp
from pyspark.sql.window import Window

# 📦 Import necessary PySpark functions

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("SessionizeWebEvents").getOrCreate()

# 📥 Load the web_events.csv dataset 
df = spark.read.option("header","True").csv("/Users/madhukhanzode/Documents/DataEngineer-InterviewPreparation/Day6/data/web_events.csv")
df.show()
df = df.withColumn("event_time", to_timestamp(col("event_time")))

windowSpec = Window.partitionBy("user_id").orderBy("event_time")

df_with_lag = df.withColumn("previous_event_time", lag(col("event_time"), 1).over(windowSpec))
df_with_lag.show()

df_gap = df_with_lag.withColumn(
    "gap_minutes",
    (unix_timestamp(col("event_time")) - unix_timestamp(col("previous_event_time"))) / 60
)
df_gap.show()

df_flagged = df_gap.withColumn(
    "session_break",
    when(col("gap_minutes").isNull() | (col("gap_minutes") > 30), 1).otherwise(0)
)
df_flagged.show()

window_session = Window.partitionBy("user_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, 0)

df_sessionized = df_flagged.withColumn(
    "session_id",
    spark_sum("session_break").over(window_session)
)

df_sessionized.select("user_id", "event_time", "page", "session_id").orderBy("user_id", "event_time").show(truncate=False)

