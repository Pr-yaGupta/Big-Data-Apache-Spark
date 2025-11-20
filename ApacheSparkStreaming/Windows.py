# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE workspace.stream.windowstb1
# MAGIC (
# MAGIC   color STRING,
# MAGIC   event_date TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.windowstb1
# MAGIC VALUES
# MAGIC ('red', '2025-01-01T11:01:00.000+00:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.stream.windowstb1

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.windowstb1
# MAGIC VALUES
# MAGIC ('green', '2025-01-01T11:07:00.000+00:00')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.stream.windowstb1

# COMMAND ----------

df = spark.readStream.table("workspace.stream.windowstb1")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.groupBy("color", window("event_date", "10 minutes"))\
        .agg(count(lit(1)).alias("color_count"))

# COMMAND ----------

df.writeStream.format("delta")\
                .outputMode("complete")\
                .trigger(once = True)\
                .option("path", "/Volumes/workspace/stream/streaming/windows/data")\
                .option("checkpointLocation", "/Volumes/workspace/stream/streaming/windows/checkpoint")\
                .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/stream/streaming/windows/data`