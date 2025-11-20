# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.readStream.table("workspace.stream.sourcetable")

# COMMAND ----------

def myfunc(df, batch_id):

    df = df.groupBy("color").agg(count("*").alias("count"))

    # destination 1
    df.write.format("delta")\
                .mode('append')\
                .option("path", "/Volumes/workspace/stream/streaming/foreachsink/dest1")\
                .save()

    # destination 2
    df.write.format("delta")\
                .mode('append')\
                .option("path", "/Volumes/workspace/stream/streaming/foreachsink/dest2")\
                .save()

# COMMAND ----------

df.writeStream.foreachBatch(myfunc)\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", "/Volumes/workspace/stream/streaming/foreachsink/checkpoint")\
    .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/stream/streaming/foreachsink/dest2`