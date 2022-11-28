# Databricks notebook source
# MAGIC %run ../../setup/initialize

# COMMAND ----------

import os
os.getcwd()
os.chdir('../..')
#to get the current working directory
directory = os.getcwd()

print(directory)

# COMMAND ----------

telematic_path = directory + "/resource/data_sources/Telematics"

# COMMAND ----------

_ = spark.sql('''
  CREATE TABLE telematics
  USING DELTA 
  LOCATION '{}'
  '''.format(telematic_path))
