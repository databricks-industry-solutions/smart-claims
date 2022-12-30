# Databricks notebook source
# MAGIC %md
# MAGIC # iot streaming data

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

iot_path = getParam('Telematics_path')

# COMMAND ----------

_ = spark.sql('''
  CREATE TABLE IF not exists telematics
  USING DELTA 
  LOCATION '{}'
  '''.format(iot_path))
