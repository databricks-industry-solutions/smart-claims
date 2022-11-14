# Databricks notebook source
import geopy
import pandas as pd
import pyspark.sql.functions as F

# location = geolocator.geocode("QUEENS 11697")
# print(location.address)
# print(location.latitude, location.longitude)
def get_lat_long(df, geolocator, address_field = "address", lat_field="latitude", long_field="longitude"):
  location = geolocator.geocode(df[address_field])
  df[lat_field] = location.latitude
  df[long_field] = location.longitude
  return df

geolocator = geopy.Nominatim(user_agent="claims_lat_long", timeout=None)

# COMMAND ----------

policy_claims_df = spark.table("smart_claims_new.silver_claims_policy")
display(policy_claims_df)

# COMMAND ----------

policy_claims_with_address = policy_claims_df.withColumn("address", F.concat(F.col("BOROUGH"), F.lit(", "), F.col("ZIP_CODE").cast("int").cast("string")))
display(policy_claims_with_address)

# COMMAND ----------

policy_claims_with_address_pd = policy_claims_with_address.where(F.col("address").isNotNull()).toPandas()
unique_address = pd.DataFrame()
unique_address["address"] = policy_claims_with_address_pd.address.unique()
unique_address = unique_address.apply(get_lat_long, axis=1, geolocator=geolocator)
display(unique_address)

# COMMAND ----------

unique_address_df = spark.createDataFrame(unique_address)

policy_claims_lat_long = policy_claims_with_address.join(unique_address_df, on="address")
display(policy_claims_lat_long)

# COMMAND ----------

policy_claims_lat_long.write.format("delta").mode("append").saveAsTable("smart_claims_new.silver_claims_policy_location")
