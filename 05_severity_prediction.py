# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims.git

# COMMAND ----------

# MAGIC %md
# MAGIC # Load claim images and score using model in MLFlow Registry

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

import os
import mlflow
from pyspark.sql.functions import regexp_extract, col, split, size

# COMMAND ----------

metadata_df = spark.read.format("csv").option("header", "true").load(getParam("Accident_metadata_path"))

# COMMAND ----------

acc_df =spark.read.format('binaryFile').load(getParam("Accidents_path"))

# COMMAND ----------

split_col=split(acc_df['path'],'/')
accident_df_id = acc_df.withColumn('image_name1',split_col.getItem(size(split_col) - 1))

# COMMAND ----------

accident_bronze_df = metadata_df.join(accident_df_id,accident_df_id.image_name1==metadata_df.image_name,"leftouter").drop("image_name1")
accident_bronze_df.write.mode("overwrite").format("delta").saveAsTable("bronze_accident")

# COMMAND ----------

accident_df = accident_df_id.toPandas()

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

# output_location = getParam("prediction_path")
# accident_df_spark.write.format("delta").mode("overwrite").save(output_location)
# spark.sql("CREATE TABLE IF NOT EXISTS accidents USING DELTA LOCATION '{}' ".format(output_location))

# COMMAND ----------

accident_metadata_df = metadata_df.join(accident_df_spark,accident_df_spark.image_name1==metadata_df.image_name,"leftouter").drop("image_name1","content","path")

# COMMAND ----------

display(accident_metadata_df)

# COMMAND ----------

output_location = getParam("prediction_path")
accident_metadata_df.write.format("delta").mode("overwrite").save(output_location)
spark.sql("CREATE TABLE IF NOT EXISTS silver_accident USING DELTA LOCATION '{}' ".format(output_location))
