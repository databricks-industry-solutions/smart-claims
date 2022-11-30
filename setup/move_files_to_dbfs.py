# Databricks notebook source
# MAGIC %cp ../resource/images/*.jpg /tmp

# COMMAND ----------

dbutils.fs.rm("/FileStore/smart_claims", True)

# COMMAND ----------

dbutils.fs.cp("file:/tmp/1_High.jpg", "dbfs:/FileStore/smart_claims/1_High.jpg")
dbutils.fs.cp("file:/tmp/1_Medium.jpg", "dbfs:/FileStore/smart_claims/1_Medium.jpg")
dbutils.fs.cp("file:/tmp/1_Low.jpg", "dbfs:/FileStore/smart_claims/1_Low.jpg")
