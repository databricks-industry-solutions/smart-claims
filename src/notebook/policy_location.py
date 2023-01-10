# Databricks notebook source
# MAGIC %md
# MAGIC # Location
# MAGIC * Uses geopy lib to add lat/long for dashboard display
# MAGIC * <b>Input Table: </b> silver_claim_policy
# MAGIC * <b> Output Table: </b> silver_claim_policy_location

# COMMAND ----------

# MAGIC %pip install geopy

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

import geopy
import pandas as pd
import pyspark.sql.functions as F

def get_lat_long(df, geolocator, address_field = "address", lat_field="latitude", long_field="longitude"):
  location = geolocator.geocode(df[address_field])
  df[lat_field] = location.latitude
  df[long_field] = location.longitude
  return df

geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=None)

# COMMAND ----------

policy_claim_df = spark.table("silver_claim_policy")
display(policy_claim_df)

# COMMAND ----------

policy_claim_with_address = policy_claim_df.withColumn("address", F.concat(F.col("BOROUGH"), F.lit(", "), F.col("ZIP_CODE").cast("int").cast("string")))
display(policy_claim_with_address)

# COMMAND ----------

policy_claim_with_address_pd = policy_claim_with_address.where(F.col("address").isNotNull()).toPandas()
unique_address = pd.DataFrame()
unique_address["address"] = policy_claim_with_address_pd.address.unique()
unique_address = unique_address.apply(get_lat_long, axis=1, geolocator=geolocator)
display(unique_address)

# COMMAND ----------

unique_address_df = spark.createDataFrame(unique_address)

policy_claim_lat_long = policy_claim_with_address.join(unique_address_df, on="address")
display(policy_claim_lat_long)

# COMMAND ----------

policy_claim_lat_long.write.format("delta").mode("append").saveAsTable("silver_claim_policy_location")

# COMMAND ----------


