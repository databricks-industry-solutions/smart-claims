# Databricks notebook source
# MAGIC %md
# MAGIC # iot streaming data

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

spark.sql("CREATE TABLE IF not exists silver_telematics USING DELTA LOCATION '{}' ".format(getParam('Telematics_path')))
