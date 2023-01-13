# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims.git

# COMMAND ----------

# MAGIC %md
# MAGIC # iot streaming data

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

spark.sql("CREATE TABLE IF not exists silver_telematics USING DELTA LOCATION '{}' ".format(getParam('Telematics_path')))
