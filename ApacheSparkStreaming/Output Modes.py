# Databricks notebook source
from delta.tables import DeltaTable

# COMMAND ----------

DeltaTable.createIfNotExists(spark)\
            .tableName("workspace.stream.sourcetable")\
            .addColumn("color", "STRING")\
            .execute()

# COMMAND ----------

DeltaTable.createIfNotExists(spark)\
            .tableName("workspace.stream.sinktable")\
            .addColumn("color", "STRING")\
            .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.stream.sourcetable VALUES
# MAGIC ('maroon')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.stream.sourcetable

# COMMAND ----------

df = spark.readStream.table('workspace.stream.sourcetable')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = df.groupBy("color").agg(count("*").alias("count"))

# COMMAND ----------

df.writeStream.format("delta")\
                .outputMode('complete')\
                .trigger(once=True)\
                .option("checkpointLocation", "/Volumes/workspace/stream/streaming/output/check")\
                .option("path", "/Volumes/workspace/stream/streaming/output/data")\
                .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/stream/streaming/output/data`