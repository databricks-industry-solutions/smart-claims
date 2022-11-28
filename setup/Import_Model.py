# Databricks notebook source
# MAGIC %md ### Import Model
# MAGIC 
# MAGIC ##### Widgets
# MAGIC * Model - new registered model name.
# MAGIC * Experiment name - contains runs created for model versions.
# MAGIC * Input folder - Input directory containing the exported model.
# MAGIC 
# MAGIC #### Setup
# MAGIC * See Setup in [README]($../_README).

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

# MAGIC %run ../setup/Common

# COMMAND ----------

import os
directory = os.getcwd()

# COMMAND ----------

model_path = directory+ '/resource/Model/'
dbutils.fs.cp(model_path, getParam(model_dbfs_path), recurse=True)
damage_severity_model_name

# COMMAND ----------

model_name = getParam(damage_severity_model_name)

 experiment_name = getParam(damage_severity_model_dir)

input_dir = getParam(getParam(model_dbfs_path)

import os
os.environ["INPUT_DIR"] = input_dir.replace("dbfs:","/dbfs")

print("model_name:",model_name)
print("input_dir:",input_dir)
print("experiment_name:",experiment_name)

# COMMAND ----------

if len(input_dir)==0: raise Exception("ERROR: Input directory is required")
if len(input_dir)==0: raise Exception("ERROR: model name is required")
if len(experiment_name)==0: raise Exception("ERROR: Destination experiment name is required")

# COMMAND ----------

# MAGIC %md ### Display model files to be imported

# COMMAND ----------

# MAGIC %sh ls -l $INPUT_DIR

# COMMAND ----------

# MAGIC %sh cat $INPUT_DIR/model.json

# COMMAND ----------

# MAGIC %md ### Import model

# COMMAND ----------

from mlflow_export_import.model.import_model import ModelImporter
importer = ModelImporter(mlflow.tracking.MlflowClient())
importer.import_model(model_name, input_dir, experiment_name, delete_model=True)

# COMMAND ----------

# MAGIC %md ### Display MLflow UI URIs

# COMMAND ----------

display_registered_model_uri(model_name)

# COMMAND ----------

display_experiment_uri(experiment_name)
