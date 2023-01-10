# Databricks notebook source
# MAGIC %md
# MAGIC * This file is inluded in all other Notebooks to get common definitions/configurations

# COMMAND ----------

import re
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### File Paths

# COMMAND ----------

main_directory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/setup')[0]

# We ensure that all objects created in that notebooks will be registered in a user specific database. 
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# Please replace this cell should you want to store data somewhere else.
database_name = '{}_smart_claims'.format(re.sub('\W', '_', username))

home_directory = '/FileStore/{}/smart_claims'.format(username)
temp_directory = "/tmp/smart_claims"
home_directory_dbfs = 'dbfs:/FileStore/{}/smart_claims'.format(username)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Metadata

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

# claims_path = main_directory + "/resource/data_sources/claims_data/Claims"
# policy_path = main_directory + "/resource/data_sources/Policies/policies.csv"

# COMMAND ----------

# telematic_path = main_directory + "/resource/data_sources/Telematics"

# COMMAND ----------

# accident_path = main_directory + "/resource/data_sources/Accidents"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use Schema

# COMMAND ----------

_ = sql("USE DATABASE {}".format(database_name))
