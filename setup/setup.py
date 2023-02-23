# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Activities
# MAGIC * This is the first notebook that should be run to setup the schema, libraries, data etc
# MAGIC * Libraries:
# MAGIC   * geopy: Get latitude/longitude from zipcode
# MAGIC   * mlflow-export-import: for importing the model into MFlow model registry
# MAGIC * Schema: 
# MAGIC   * user specific eg. {username}_smart_claims
# MAGIC * File Paths:
# MAGIC   * home_directory = '/FileStore/{}/smart_claims'.format(username)
# MAGIC   * temp_directory = "/tmp/{}/smart_claims".format(username)
# MAGIC * Model
# MAGIC   * Model - new registered model name.
# MAGIC   * Experiment name - contains runs created for model versions.
# MAGIC   * Input folder - Input directory containing the exported model.
# MAGIC * Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Libraries

# COMMAND ----------

# MAGIC %pip install geopy git+https:///github.com/amesar/mlflow-export-import/#egg=mlflow-export-import

# COMMAND ----------

import re
from pathlib import Path
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema & File Path Names

# COMMAND ----------

main_directory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/setup')[0]
# We ensure that all objects created in that notebooks will be registered in a user specific database. 
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Please replace this cell should you want to store data somewhere else.
# database_name = '{}_smart_claims'.format(re.sub('\W', '_', username))
database_name = 'smart_claims'

home_directory = '/FileStore/{}/smart_claims'.format(username)
temp_directory = "/tmp/smart_claims"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration

# COMMAND ----------

config = {
  'home_dir' : home_directory,
  'temp_dir' : temp_directory,
  'dlt_path': '{}/dlt'.format(home_directory),
  'Telematics_path': '{}/data_sources/Telematics'.format(temp_directory),
  'Policy_path': '{}/data_sources/Policy'.format(temp_directory),
  'Claims_path': '{}/data_sources/Claims'.format(temp_directory),
  'Accidents_path': '{}/data_sources/Accidents'.format(temp_directory),
  'Accident_metadata_path': '{}/data_sources/Accident_metadata'.format(temp_directory),
  'prediction_path': '{}/data_sources/predictions_delta'.format(home_directory),
  'model_dir_on_dbfs' : 'dbfs:/FileStore/{}/severity_model/Model'.format(username),
  'image_dir_on_dbfs' : 'dbfs:/FileStore/smart_claims',
  'damage_severity_model_dir'    :  '/Users/{}/car_damage_severity'.format(user),
  'damage_severity_model_name'   :  'damage_severity_{}'.format(re.sub('\.', '_', username)),
  'sql_warehouse_id' : ""  
}

def getParam(s):
  return config[s]
 
# passing configuration to scala
spark.createDataFrame(pd.DataFrame(config, index=[0])).createOrReplaceTempView('smart_claims_config')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tear down & Setup (schema & file paths)

# COMMAND ----------

from pathlib import Path

def tear_down():
  import shutil
  try:
    shutil.rmtree(temp_directory)
  except:
    pass
  dbutils.fs.rm(home_directory, True)
  spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database_name))
  dbutils.fs.rm(getParam("model_dir_on_dbfs"),recurse=True)
  dbutils.fs.rm(getParam("image_dir_on_dbfs"),recurse=True)
  dbutils.fs.rm(getParam("damage_severity_model_dir"),recurse=True)
  dbutils.fs.rm(getParam("home_dir"),recurse=True)
  dbutils.fs.rm(getParam("temp_dir"),recurse=True)
  
def setup():
  spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
  spark.sql("USE DATABASE {}".format(database_name))

  # Similar to database, we will store actual content on a given path
  dbutils.fs.mkdirs(home_directory)
  dbutils.fs.mkdirs(temp_directory)

#   # Where we might stored temporary data on local disk
#   Path(temp_directory).mkdir(parents=True, exist_ok=True)



tear_down()
setup()

# COMMAND ----------

# MAGIC %run ./initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy model & images to driver /tmp

# COMMAND ----------

# MAGIC %sh -e
# MAGIC cd /databricks/driver/
# MAGIC wget -O resource.zip https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource_bundle.zip
# MAGIC unzip -o resource.zip -d tmp/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy model & images from driver /tmp to dbfs

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Model", getParam("model_dir_on_dbfs"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/images", getParam("image_dir_on_dbfs"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/images", getParam("Accidents_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/image_metadata", getParam("Accident_metadata_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Telematics", getParam("Telematics_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Policy", getParam("Policy_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Claims", getParam("Claims_path"),recurse=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /databricks/driver/tmp/resource_bundle/
# MAGIC rm -r images
# MAGIC rm -r Model
# MAGIC rm -r Telematics
# MAGIC rm -r Claims
# MAGIC rm -r Policy
# MAGIC rm -r image_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import model from dbfs to MLFlow registry

# COMMAND ----------

# MAGIC %run ./import_model
