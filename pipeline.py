# Databricks notebook source
from pyspark.sql.functions import sum, col, when

# COMMAND ----------

orders_df = spark.table("transportation.default.orders")
customers_df = spark.table("transportation.default.customers")
products_df = spark.table("transportation.default.products")



# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.show(3)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(orders_df.describe())

# COMMAND ----------

orders_df.count()

# COMMAND ----------

len(orders_df.columns)

# COMMAND ----------

orders_df.columns

# COMMAND ----------


products_df.columns

# COMMAND ----------

customers_df.columns

# COMMAND ----------

Orders_df = orders_df \
    .withColumnRenamed("order_id", "Order_id") \
    .withColumnRenamed("customer_id", "Customer_id") \
    .withColumnRenamed("product_id", "Product_id") \
    .withColumnRenamed("amount", "AMOUNT") \
    .withColumnRenamed("order_date", "DATEOFORDER") \
    .withColumnRenamed("cost_price", "Cost_price")

# COMMAND ----------

Orders_df = Orders_df.withColumn(
    "AMOUNT",
    col("AMOUNT").cast("INT")
)

# COMMAND ----------

Orders_df=Orders_df.withColumn("Profit", col('AMOUNT')-col('Cost_price'))

# COMMAND ----------

Orders_df.select("AMOUNT", "Cost_price").show()

# COMMAND ----------

display(Orders_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC ###Column filtering

# COMMAND ----------

trimmed_df = Orders_df.select("Order_id", "AMOUNT", "DATEOFORDER")
display(trimmed_df)

# COMMAND ----------

# MAGIC %md ###Row filtering
# MAGIC

# COMMAND ----------

filtered_orders = Orders_df.filter(
    col("DATEOFORDER").between("2024-01-01", "2024-01-03")
)

display(filtered_orders)

# COMMAND ----------

filtered2_order=Orders_df.select("Customer_id").distinct()
display(filtered2_order)

# COMMAND ----------

filtered3_order=Orders_df.dropDuplicates
display(filtered3_order)

# COMMAND ----------

from pyspark.sql.functions import when, col

Orders_dirty = Orders_df.withColumn(
    "AMOUNT",
    when(col("Order_id") == 2, None)
    .when(col("Order_id") == 3, -100)
    .otherwise(col("AMOUNT")),
)

display(Orders_dirty)

# COMMAND ----------

null_counts_orders_dirty = Orders_dirty.select(
    [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in Orders_dirty.columns]
)
display(null_counts_orders_dirty)


# COMMAND ----------

display(Orders_df)

# COMMAND ----------

Orders_clean = Orders_dirty.fillna("Unknown")

Orders_clean = Orders_clean.filter(col("amount") > 0) # remove negative



display(Orders_clean)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

display(products_df)

# COMMAND ----------

df_join = Orders_clean.join(customers_df, "customer_id")
df_final = df_join.join(products_df, "product_id")

display(df_final)

# COMMAND ----------

df_final.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("transportation.default.orders_clean")

# COMMAND ----------

from pyspark.sql.functions import sum

sales_df = df_final.groupBy("category").agg(sum("AMOUNT").alias("total_sales"))

display(sales_df)

# COMMAND ----------

sales_df.write.mode("overwrite").saveAsTable("transportation.default.sales")

# COMMAND ----------

spark.table("transportation.default.orders_clean").show()
spark.table("transportation.default.sales").show()