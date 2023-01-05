# Databricks notebook source
# MAGIC %md
# MAGIC # Load claim images and score using model in MLFlow Registry

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

import os
import mlflow

# COMMAND ----------

accident_df = (spark.sql("select * from silver_claims_policy").toPandas())

# COMMAND ----------

model_production_uri = "models:/{}/production".format(getParam("damage_severity_model_name"))
 
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_production_uri))
wrapper = mlflow.pyfunc.load_model(model_production_uri)

# COMMAND ----------

# wrapper = mlflow.pyfunc.load_model(model_production)
accident_df['severity'] = wrapper.predict(accident_df['content'])

# COMMAND ----------

accident_df_spark = spark.createDataFrame(accident_df)

# COMMAND ----------

output_location = getParam("prediction_path")
accident_df_spark.write.format("delta").mode("overwrite").save(output_location)
spark.sql("CREATE TABLE IF NOT EXISTS accidents USING DELTA LOCATION '{}' ".format(output_location))

# COMMAND ----------

display(accident_df_spark)
