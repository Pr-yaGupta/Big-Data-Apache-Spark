# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read Json Data (Batch)**

# COMMAND ----------

df = spark.read.format("json")\
                .option("inferSchema", True)\
                .option("multiline", True)\
                .load("/Volumes/workspace/stream/streaming/jsonsource/day1.json")
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("order_id", "timestamp").display()

# COMMAND ----------

df.select("order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country").display()

# COMMAND ----------

df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country", "payment", "metadata")

df = df.withColumn("items", explode_outer("items"))

display(df)

# COMMAND ----------

df = df.withColumn("item_id", col("items").item_id)\
        .withColumn("product_name", col("items").product_name)\
        .withColumn("price", col("items").price)\
        .withColumn("quantity", col("items").quantity)\
        .drop("items")

df.display()

# COMMAND ----------

df = df.withColumn("payment_method", col("payment").method)\
        .withColumn("transaction_id", col("payment").transaction_id)\
        .drop("payment")

display(df)

# COMMAND ----------

df = df.withColumn("metadata", explode_outer("metadata"))

display(df)

# COMMAND ----------

df = df.withColumn("key", col("metadata").key)\
        .withColumn("value", col("metadata").value)\
        .drop("metadata")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read Streaming Data**

# COMMAND ----------

my_schema = """order_id STRING,
        timestamp STRING,
        customer STRUCT<
          customer_id: INT,
          name: STRING,
          email: STRING,
          address: STRUCT<
            city: STRING,
            postal_code: STRING,
            country: STRING
          >
        >,
        items ARRAY<STRUCT<
          item_id: STRING,
          product_name: STRING,
          quantity: INT,
          price: DOUBLE
        >>,
        payment STRUCT<
          method: STRING,
          transaction_id: STRING
        >,
        metadata ARRAY<STRUCT<
          key: STRING,
          value: STRING
        >>"""

# COMMAND ----------

df = spark.readStream.format("json")\
                .option("multiline", True)\
                .schema(my_schema)\
                .load("/Volumes/workspace/stream/streaming/jsonsource/")

df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country", "payment", "metadata")

df = df.withColumn("items", explode_outer("items"))

df = df.withColumn("item_id", col("items").item_id)\
        .withColumn("product_name", col("items").product_name)\
        .withColumn("price", col("items").price)\
        .withColumn("quantity", col("items").quantity)\
        .drop("items")

df = df.withColumn("payment_method", col("payment").method)\
        .withColumn("transaction_id", col("payment").transaction_id)\
        .drop("payment")

df = df.withColumn("metadata", explode_outer("metadata"))

df = df.withColumn("key", col("metadata").key)\
        .withColumn("value", col("metadata").value)\
        .drop("metadata")


# COMMAND ----------

df.writeStream.format("delta")\
            .outputMode("append")\
            .trigger(once = True)\
            .option("path", "/Volumes/workspace/stream/streaming/jsonsink/Data")\
            .option("checkpointLocation", "/Volumes/workspace/stream/streaming/jsonsink/checkpoint")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/stream/streaming/jsonsink/Data/`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Archiving**

# COMMAND ----------

df = spark.readStream.format("json")\
                .option("multiline", True)\
                .schema(my_schema)\
                .option("cleanSource", "archive")\
                .option("sourceArchiveDir", "/Volumes/workspace/stream/streaming/jsonsourcearchive")\
                .load("/Volumes/workspace/stream/streaming/jsonsourcenew")

df = df.select("items", "order_id", "timestamp", "customer.customer_id", "customer.name", "customer.email", "customer.address.city", "customer.address.postal_code", "customer.address.country", "payment", "metadata")

df = df.withColumn("items", explode_outer("items"))

df = df.withColumn("item_id", col("items").item_id)\
        .withColumn("product_name", col("items").product_name)\
        .withColumn("price", col("items").price)\
        .withColumn("quantity", col("items").quantity)\
        .drop("items")

df = df.withColumn("payment_method", col("payment").method)\
        .withColumn("transaction_id", col("payment").transaction_id)\
        .drop("payment")

df = df.withColumn("metadata", explode_outer("metadata"))

df = df.withColumn("key", col("metadata").key)\
        .withColumn("value", col("metadata").value)\
        .drop("metadata")


# COMMAND ----------

df.writeStream.format("delta")\
            .outputMode("append")\
            .trigger(once = True)\
            .option("path", "/Volumes/workspace/stream/streaming/jsonsinknew/Data")\
            .option("checkpointLocation", "/Volumes/workspace/stream/streaming/jsonsinknew/checkpoint")\
            .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/stream/streaming/jsonsinknew/Data/`