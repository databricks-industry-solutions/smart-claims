# Databricks notebook source
# MAGIC %pip install geopy

# COMMAND ----------

import re
from pathlib import Path
import pandas as pd

# COMMAND ----------

config = {
  'driver_to_dbfs_path_claims'   :,
  'driver_to_dbfs_path_policy'   :,
  'driver_to_dbfs_path_telematic'
  'damage_severity_model_dir'    :  '{}/fasttext'.format(home_directory),
  'damage_severity_model_name'   :  'damage_severity_{}'.format(re.sub('\.', '_', username)),
  'sql_warehouse_id' : ,
  
}

# COMMAND ----------

def getParam(s):
  return config[s]
 
# passing configuration to scala
spark.createDataFrame(pd.DataFrame(config, index=[0])).createOrReplaceTempView('smart_claims_config')

# COMMAND ----------

# We ensure that all objects created in that notebooks will be registered in a user specific database. 
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]

# Please replace this cell should you want to store data somewhere else.
database_name = '{}_smart_claims'.format(re.sub('\W', '_', username))

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaanup prior run state

# COMMAND ----------

def tear_down():
  import shutil
  try:
    shutil.rmtree(temp_directory)
  except:
    pass
  dbutils.fs.rm(home_directory, True)
  _ = sql("DROP DATABASE IF EXISTS {} CASCADE".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

tear_down()

# COMMAND ----------

_ = sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
_ = sql("USE DATABASE {}".format(database_name))

# Similar to database, we will store actual content on a given path
home_directory = '/FileStore/{}/smart_claims'.format(username)
dbutils.fs.mkdirs(home_directory)

# Where we might stored temporary data on local disk
temp_directory = "/tmp/{}/smart_claims".format(username)
Path(temp_directory).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

dbutils.fs.rm(getParam("transactions"),recurse=True)
dbutils.fs.rm(getParam("transactions_fasttext"),recurse=True)
dbutils.fs.rm(getParam("transactions_train_raw"),recurse=True)
dbutils.fs.rm(getParam("transactions_train_hex"),recurse=True)
dbutils.fs.rm(getParam("transactions_valid_raw"),recurse=True)
dbutils.fs.rm(getParam("transactions_valid_hex"),recurse=True)
dbutils.fs.rm(getParam("transactions_valid_hex"),recurse=True)
dbutils.fs.rm(getParam("transactions_model_dir"),recurse=True)
dbutils.fs.rm(getParam("merchant_edges"),recurse=True)
dbutils.fs.rm(getParam("merchant_nodes"),recurse=True)
dbutils.fs.rm(getParam("shopping_trips"),recurse=True)
dbutils.fs.rm(getParam("merchant_vectors"),recurse=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv

# COMMAND ----------

# MAGIC %sh

# COMMAND ----------

# MAGIC %sh

# COMMAND ----------

driver_to_dbfs_path_claims = 'dbfs:/home/{}/ibm-telco-churn/Telco-Customer-Churn.csv'.format(user)
dbutils.fs.cp('file:/databricks/driver/Telco-Customer-Churn.csv', driver_to_dbfs_path)

# COMMAND ----------


