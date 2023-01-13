# Databricks notebook source
# MAGIC %md
# MAGIC # Import Model 
# MAGIC * Pretrained model in github is put into dbfs (during one-time setup)
# MAGIC * Here the dbfs model is put into the model registry for consumption during pipeline ingestion/inferencing

# COMMAND ----------

# MAGIC %run ./initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLFlow Utility Functions

# COMMAND ----------

import mlflow
client = mlflow.tracking.MlflowClient()
host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName")

def display_experiment_uri(experiment_name):
    if host_name:
        experiment_id = client.get_experiment_by_name(experiment_name).experiment_id
        uri = "https://{}/#mlflow/experiments/{}".format(host_name, experiment_id)
        displayHTML("""<b>Experiment URI:</b> <a href="{}">{}</a>""".format(uri,uri))
        
def display_registered_model_uri(model_name):
    if host_name:
        uri = f"https://{host_name}/#mlflow/models/{model_name}"
        displayHTML("""<b>Registered Model URI:</b> <a href="{}">{}</a>""".format(uri,uri))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Metadata

# COMMAND ----------

model_name = getParam("damage_severity_model_name")
if len(model_name)==0: raise Exception("ERROR: model name is required")
print("model_name:",model_name)

experiment_name = getParam("damage_severity_model_dir")
if len(experiment_name)==0: raise Exception("ERROR: Destination experiment name is required")
print("experiment_name:",experiment_name)

input_dir = getParam("model_dir_on_dbfs")
if len(input_dir)==0: raise Exception("ERROR: Input directory is required")
print("input_dir:",input_dir)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Import model

# COMMAND ----------

from mlflow_export_import.model.import_model import ModelImporter

importer = ModelImporter(mlflow.tracking.MlflowClient())
importer.import_model(model_name, input_dir, experiment_name, delete_model=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Display Model in MLflow 

# COMMAND ----------

display_registered_model_uri(model_name)

# COMMAND ----------

display_experiment_uri(experiment_name)
