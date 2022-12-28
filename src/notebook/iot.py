# Databricks notebook source
# MAGIC %md
# MAGIC # iot streaming data

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

_ = spark.sql('''
  CREATE TABLE IF not exists telematics
  USING DELTA 
  LOCATION '{}'
  '''.format(telematic_path))
