# Databricks notebook source
# MAGIC %md
# MAGIC # Load claim images and score using model in MLFlow Registry

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

# MAGIC %run ../../setup/import_model

# COMMAND ----------

import os
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
import mlflow

# COMMAND ----------

accident_path = main_directory + "/resource/data_sources/Accident"

# COMMAND ----------

acc_df =spark.read.format('binaryFile').load(accident_path)

# COMMAND ----------

w = Window.partitionBy(lit(1)).orderBy("length")
accident_df = (acc_df.withColumn("driver_id", row_number().over(w)).toPandas())

# COMMAND ----------

model_production_uri = "models:/{model_name}/production".format(model_name=model_name)
 
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_production_uri))
model_production = mlflow.pyfunc.load_model(model_production_uri)

# COMMAND ----------

wrapper = mlflow.pyfunc.load_model("runs:/{}/pipeline".format(run_id))
accident_df['severity'] = wrapper.predict(accident_df['content'])

# COMMAND ----------

accident_df_spark = spark.createDataFrame(accident_df)

# COMMAND ----------

accident_df_spark.write.format("delta").mode("overwrite").save("/FileStore/marzi/claims_data/Accident_delta")
spark.sql("CREATE TABLE IF NOT EXISTS accidents USING DELTA LOCATION '/FileStore/marzi/claims_data/Accident_delta' ")

# COMMAND ----------

display(accident_df_spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH numberList as
# MAGIC (
# MAGIC    SELECT <input number here> AS NUM
# MAGIC    UNION ALL
# MAGIC    SELECT NUM-1 
# MAGIC    FROM numberList
# MAGIC    WHERE NUM-1 >= 0
# MAGIC )
# MAGIC SELECT t.name, t.value, numberList.num
# MAGIC FROM smart_claims.accident_2, 5
