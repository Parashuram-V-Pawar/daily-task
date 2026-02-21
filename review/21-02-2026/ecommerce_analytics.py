# E-Commerce ETL and Analytics
# Using orders.csv and order_items.csv:
# 1. Upload data to HDFS (/ecommerce/raw/)
# 2. Read CSV files into Spark DataFrames
# 3. Join datasets and perform data cleaning
# 4. Perform analytics:
#    - Total revenue
#    - Revenue per month
#    - Top 5 selling products
#    - Average order value
# 5. Store all outputs in Parquet format under /ecommerce/analytics/

#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import subprocess

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder.appName("Ecommerce ETL and Analytics").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Creating HDFS Directories
#---------------------------------------------------------------------------
command = 'hdfs dfs -mkdir -p /ecommerce'
subprocess.run(command, shell=True)
command = 'hdfs dfs -mkdir -p /ecommerce/raw'
subprocess.run(command, shell=True)
# upload orders.csv to HDFS
command = f'hdfs dfs -put data/orders.csv /ecommerce/raw'
subprocess.run(command, shell=True)
# upload orders_dataset.csv to HDFS
command = f'hdfs dfs -put data/order_items.csv /ecommerce/raw'
subprocess.run(command, shell=True)

command = 'hdfs dfs -mkdir -p /ecommerce/analytics'
subprocess.run(command, shell=True)

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
RAW_PATH = "hdfs:///ecommerce/raw/"
ANALYTICS_PATH = "hdfs:///ecommerce/analytics/"

#---------------------------------------------------------------------------
# Step 2: Load CSV Files
#---------------------------------------------------------------------------
orders = spark.read.csv(
    RAW_PATH + "orders.csv",
    header=True,
    inferSchema=True
)

order_items = spark.read.csv(
    RAW_PATH + "order_items.csv",
    header=True,
    inferSchema=True
)

print("Orders Schema")
orders.printSchema()

print("Order Items Schema")
order_items.printSchema()

print("Number of records in orders:", orders.count())
print("Number of records in order_items:", order_items.count())

#---------------------------------------------------------------------------
# Step 3: Join Data and Cleaning
#---------------------------------------------------------------------------
full_data = orders.join(
    order_items,
    "order_id",
    "inner"
)

# Handle Null Values
full_data_cleaned = full_data.dropna(
    subset=["order_id", "price"]
)

# Convert date columns
full_data_cleaned = full_data_cleaned \
    .withColumn("order_purchase_timestamp",
                to_date(col("order_purchase_timestamp"))) \
    .withColumn("order_delivered_carrier_date",
                to_date(col("order_delivered_carrier_date"))) \
    .withColumn("order_delivered_customer_date",
                to_date(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date",
                to_date(col("order_estimated_delivery_date")))

print("Joined and Cleaned Data")
full_data_cleaned.show(5)

#---------------------------------------------------------------------------
# Step 4: Perform Analytics
#---------------------------------------------------------------------------

# Add Total Price Column
full_data_cleaned = full_data_cleaned.withColumn(
    "Total price",
    col("price") + col("freight_value")
)

#--------------------------------------------------
# 1. Total Revenue
#--------------------------------------------------
print("Total Revenue")

total_revenue = full_data_cleaned.agg(
    sum("Total price").alias("Total Revenue")
)

total_revenue = total_revenue.withColumn(
    "Total Revenue",
    col("Total Revenue").cast(DecimalType(10,2))
)

total_revenue.show()

#--------------------------------------------------
# 2. Monthly Revenue
#--------------------------------------------------
print("Monthly Revenue")

full_data_cleaned = full_data_cleaned.withColumn(
    "order month",
    date_format("order_purchase_timestamp", "yyyy-MM")
)

monthly_revenue = full_data_cleaned.groupBy("order month") \
    .agg(sum("Total price").alias("Monthly Revenue"))

monthly_revenue.show()

#--------------------------------------------------
# 3. Top 5 Selling Products
#--------------------------------------------------
print("Top 5 Selling Products")

top_selling_products = full_data_cleaned.groupBy("product_id") \
    .agg(count("product_id").alias("total_sold")) \
    .orderBy(col("total_sold").desc())

top_selling_products.limit(5).show()

#--------------------------------------------------
# 4. Average Order Value
#--------------------------------------------------
print("Average Order Value")

order_value = full_data_cleaned.groupBy("order_id") \
    .agg(sum("Total price").alias("Order_value"))

average_order_value = order_value.agg(
    avg("Order_value").alias("average_order_value")
)

average_order_value.show()

#---------------------------------------------------------------------------
# Step 5: Save Results to HDFS (Parquet Format)
#---------------------------------------------------------------------------

full_data_cleaned.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "full_data_cleaned")

total_revenue.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "total_revenue")

monthly_revenue.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "monthly_revenue")

top_selling_products.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "top_selling_products")

average_order_value.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "average_order_value")