# Databricks notebook source
# MAGIC %run ../../setup/initialize

# COMMAND ----------

telematic_path = main_directory + "/resource/data_sources/Telematics"

# COMMAND ----------

_ = spark.sql('''
  CREATE TABLE telematics
  USING DELTA 
  LOCATION '{}'
  '''.format(telematic_path))
