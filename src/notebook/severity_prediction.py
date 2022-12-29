# Databricks notebook source
# MAGIC %md
# MAGIC # Load claim images and score using model in MLFlow Registry

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

import os
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
import mlflow

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

output_location = getParam(model_output_severity_location)
accident_df_spark.write.format("delta").mode("overwrite").save(output_location)
spark.sql("CREATE TABLE IF NOT EXISTS accidents USING DELTA LOCATION '{}'.format(output_location) ")

# COMMAND ----------

display(accident_df_spark)

# COMMAND ----------

# %sql
# WITH numberList as
# (
#    SELECT <input number here> AS NUM
#    UNION ALL
#    SELECT NUM-1 
#    FROM numberList
#    WHERE NUM-1 >= 0
# )
# SELECT t.name, t.value, numberList.num
# FROM smart_claims.accident_2, 5
