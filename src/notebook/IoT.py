# Databricks notebook source
# MAGIC %run ../../setup/initialize

# COMMAND ----------

telematic_path = "../../resource/data_sources/Telematics"

# COMMAND ----------

iot_df = spark.read.format("delta").load(telematic_path).write.format("delta").overwite.save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists smart_claims_new.telematics
# MAGIC using delta location

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from smart_claims_new.telematics
