# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Activities
# MAGIC * This is the first notebook that should be run to setup the schema, libraries, data etc
# MAGIC * Libraries:
# MAGIC   * geopy: Get latitude/longitude from zipcode
# MAGIC   * mlflow-export-import: for importing the model into MFlow model registry
# MAGIC * Model
# MAGIC   * Model - new registered model name.
# MAGIC   * Experiment name - contains runs created for model versions.
# MAGIC   * Input folder - Input directory containing the exported model.
# MAGIC * Schema: 
# MAGIC   * user specific eg. {username}_smart_claims
# MAGIC * File Paths:
# MAGIC   * home_directory = '/FileStore/{}/smart_claims'.format(username)
# MAGIC   * temp_directory = "/tmp/{}/smart_claims".format(username)
# MAGIC  

# COMMAND ----------

import re
from pathlib import Path
import pandas as pd

# COMMAND ----------

# MAGIC %pip install geopy

# COMMAND ----------

# MAGIC %pip install git+https:///github.com/amesar/mlflow-export-import/#egg=mlflow-export-import

# COMMAND ----------

main_directory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/setup')[0]

# COMMAND ----------

# We ensure that all objects created in that notebooks will be registered in a user specific database. 
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Please replace this cell should you want to store data somewhere else.
database_name = '{}_smart_claims'.format(re.sub('\W', '_', username))

home_directory = '/FileStore/{}/smart_claims'.format(username)
temp_directory = "/tmp/{}/smart_claims".format(username)

# COMMAND ----------

config = {
  'dlt_path': '{}/dlt'.format(home_directory),
  'model_dir_on_dbfs' : 'dbfs:/FileStore/{}/severity_model/Model'.format(username),
  'image_dir_on_dbfs' : 'dbfs:/FileStore/smart_claims',
  'damage_severity_model_dir'    :  '/Users/{}/car_damage_severity'.format(user),
  'damage_severity_model_name'   :  'damage_severity_{}'.format(re.sub('\.', '_', username)),
  'sql_warehouse_id' : ""  
}

# COMMAND ----------

def getParam(s):
  return config[s]
 
# passing configuration to scala
spark.createDataFrame(pd.DataFrame(config, index=[0])).createOrReplaceTempView('smart_claims_config')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean up prior run state

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
# MAGIC #### Setup

# COMMAND ----------

tear_down()

# COMMAND ----------

_ = sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
_ = sql("USE DATABASE {}".format(database_name))

# Similar to database, we will store actual content on a given path
dbutils.fs.mkdirs(home_directory)

# Where we might stored temporary data on local disk
Path(temp_directory).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# dbutils.fs.rm(getParam("dbfs_path_claims"),recurse=True)
# dbutils.fs.rm(getParam("dbfs_path_policy"),recurse=True)
# dbutils.fs.rm(getParam("dbfs_path_telematic"),recurse=True)
# dbutils.fs.rm(getParam("dlt_path"),recurse=True)
dbutils.fs.rm(getParam("model_dir_on_dbfs"),recurse=True)
dbutils.fs.rm(getParam("image_dir_on_dbfs"),recurse=True)

# COMMAND ----------

# MAGIC 
# MAGIC %cp -r ../resource/Model /tmp/

# COMMAND ----------

# MAGIC %mkdir /tmp/images
# MAGIC %cp ../resource/data_sources/Accidents/*.jpg /tmp/images

# COMMAND ----------

dbutils.fs.cp("file:/tmp/Model", getParam("model_dir_on_dbfs"),recurse=True)
dbutils.fs.cp("file:/tmp/images", getParam("image_dir_on_dbfs"),recurse=True)

# COMMAND ----------

# MAGIC %rm -r /tmp/images

# COMMAND ----------

# MAGIC %rm -r /tmp/Model