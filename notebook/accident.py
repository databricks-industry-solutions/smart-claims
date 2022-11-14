# Databricks notebook source
# bring in images and pass them through the model built in mlflow to get the score

# COMMAND ----------

accident_delta_path = "dbfs:/FileStore/marzi/claims_data/Accident_delta"

# COMMAND ----------

acc_df =spark.read.format('binaryFile').load('/mnt/industry-gtm/fsi/datasets/car_accidents/accidents')

# COMMAND ----------

display(acc_df)

# COMMAND ----------

from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window

w = Window.partitionBy(lit(1)).orderBy("length")
accident_df = (acc_df.withColumn("driver_id", row_number().over(w)).toPandas())

# COMMAND ----------

import mlflow

# COMMAND ----------

run_id = "0c642fe2440a4d6b86d1054f9abf84e6"

# COMMAND ----------

wrapper = mlflow.pyfunc.load_model("runs:/{}/pipeline".format(run_id))
accident_df['severity'] = wrapper.predict(accident_df['content'])

# COMMAND ----------

accident_df_spark = spark.createDataFrame(accident_df)

# COMMAND ----------

accident_df_spark.write.format("delta").mode("overwrite").save("/FileStore/marzi/claims_data/Accident_delta")
spark.sql("CREATE TABLE IF NOT EXISTS smart_claims.accident USING DELTA LOCATION '/FileStore/marzi/claims_data/Accident_delta' ")

# COMMAND ----------

display(accident_df_spark)

# COMMAND ----------


