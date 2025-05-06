from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession
spark = SparkSession.builder.master("local").appName("ProductsNeverPurchased").getOrCreate()

# Sample data for products
products_data = [
    (101, "iPhone 15"),
    (102, "MacBook Pro"),
    (103, "AirPods Pro"),
    (104, "iPad Air")
]

products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False)
])

df_products = spark.createDataFrame(products_data, schema=products_schema)
df_products.show()

# Sample data for sales
sales_data = [
    (1, 101, 300, "2024-04-10"),
    (2, 103, 100, "2024-04-10"),
    (3, 101, 400, "2024-04-11")
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("revenue", IntegerType(), False),
    StructField("sale_date", StringType(), False)
])

df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.show()

df_result = df_products.join(df_sales, on="product_id", how="leftanti")
df_result.show()