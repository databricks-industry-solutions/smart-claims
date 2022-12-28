# Databricks notebook source
# MAGIC %md
# MAGIC # Rule Engine 
# MAGIC * These are pre-defined static checks that can be applied without requiring a human in the loop, thereby speeding up routine cases
# MAGIC * When the reported data does not comply with auto detected info, flags are raised to involve additionaal human investigation
# MAGIC   * Eg. Checks on policy coverage, assessed severity, accident location and speed limit violations
# MAGIC * <b>Input Table:</b> claim_policy_accident
# MAGIC * <b>Rules Table:</b> claims_rules
# MAGIC * <b>Output Table:</b> claim_policy_accident_insights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Rules
# MAGIC * Ability to dynamically add/edit rules to meet bsiness requirements around claim processing 
# MAGIC * Rules are persisted in claim_rules and applied on new data in a generic pattern as prescribed in the rule definition
# MAGIC * Rule definition inludes a
# MAGIC   * Unique Rule name/id
# MAGIC   * Definition of acceptable and not aceptable data - written as code that can be directly applied
# MAGIC   * Severity (HIGH, MEDIUM, LOW)
# MAGIC   * Is_Activ (True/False)
# MAGIC * Some common checks include
# MAGIC   * Claim date should be within coverage period
# MAGIC   * Reported Severity should match ML predicted severity
# MAGIC   * Accident Location as reported by telematics data should match the location as reported in claim
# MAGIC   * Speed limit as reported by telematics should be within speed limits of that region if there is a dispute on who was on the offense 

# COMMAND ----------

# MAGIC %run ../../setup/initialize

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS claims_rules;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS claims_policy_accident;
# MAGIC DROP TABLE IF EXISTS claims_policy_accident_insights_;
# MAGIC DROP TABLE IF EXISTS policy_claims_iot_available_insights;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS temp2_claims_policy_accident;
# MAGIC DROP TABLE IF EXISTS temp3_claims_policy_accident;
# MAGIC DROP TABLE IF EXISTS temp4_claims_policy_accident;
# MAGIC DROP TABLE IF EXISTS temp5_claims_policy_accident;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists claims_rules;
# MAGIC CREATE TABLE IF NOT EXISTS claims_rules (
# MAGIC   rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   rule_name STRING, 
# MAGIC   check_code STRING,
# MAGIC   check_severity STRING,
# MAGIC   is_active Boolean
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # Configure Rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invalid Policy Date

# COMMAND ----------

invalid_policy_date = '''
CASE WHEN to_date(pol_eff_date, "dd-MM-yyyy") < to_date(claim_datetime) and to_date(pol_expiry_date, "dd-MM-yyyy") < to_date(claim_datetime) THEN "VALID" 
ELSE "NOT VALID"  
END
'''

s_sql = "INSERT INTO claims_rules(rule_name,check_code, check_severity, is_active) values('invalid_policy_date', '" + invalid_policy_date + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exceeds Policy Amount

# COMMAND ----------

exceeds_policy_amount = '''
CASE WHEN  sum_insured_imputed >= claim_amount_total 
    THEN "calim value in the range of premium"
    ELSE "claim value more than premium"
END 
'''

s_sql = "INSERT INTO claims_rules(rule_name,check_code, check_severity,is_active) values('exceeds_policy_amount', '" + exceeds_policy_amount + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Severity Mismatch

# COMMAND ----------

severity_mismatch = '''
CASE WHEN  incident_severity="Total Loss" AND severity > 0.9 THEN  "Severity matches the report"
       WHEN  incident_severity="Major Damage" AND severity > 0.8 THEN  "Severity matches the report"
       WHEN  incident_severity="Minor Damage" AND severity > 0.7 THEN  "Severity matches the report"
       WHEN  incident_severity="Trivial Damage" AND severity > 0.4 THEN  "Severity matches the report"
       ELSE "Severity does not match"
END 
'''

s_sql = "INSERT INTO claims_rules(rule_name,check_code, check_severity, is_active) values('severity_mismatch', '" + severity_mismatch + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exceeds Speed

# COMMAND ----------

exceeds_speed = '''
CASE WHEN  speed <= 45 and speed > 0 THEN  "Normal Speed"
       WHEN speed > 45 THEN  "High Speed"
       ELSE "Invalid speed"
END
'''

s_sql = "INSERT INTO claims_rules(rule_name,check_code, check_severity,is_active) values('exceeds_speed', '" + exceeds_speed + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from claims_rules order by rule_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Dynamic Application of Rules 

# COMMAND ----------

def applyRule(pol_eff_date, claim_datetime, pol_expiry_date):
    return "Valid"

ruleUDF = udf(lambda a,b,c: applyRule(a,b,c))

# COMMAND ----------

df = spark.sql("SELECT * FROM smart_claims.claims_policy_accident limit 5")
display(df)

# COMMAND ----------

df1=df.select(col("*"), ruleUDF('pol_eff_date', 'claim_datetime', 'pol_expiry_date').alias("valid_date"))
display(df1)

# COMMAND ----------





# COMMAND ----------

rules_df = spark.sql("select rule_name, check_code from claims_rules where is_active=True")
display(rules_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE temp2_claims_policy_accident AS
# MAGIC (SELECT *,
# MAGIC   CASE WHEN to_date(pol_eff_date, "dd-MM-yyyy") < to_date(claim_datetime) and to_date(pol_expiry_date, "dd-MM-yyyy") < to_date(claim_datetime)
# MAGIC     THEN 'VALID' 
# MAGIC     ELSE 'NOT VALID' 
# MAGIC   END 
# MAGIC   AS valid_date
# MAGIC FROM temp_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check coverage of policy and the accident amount
# MAGIC CREATE TABLE temp3_claims_policy_accident AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  sum_insured_imputed >= claim_amount_total 
# MAGIC     THEN 'calim value in the range of premium' 
# MAGIC     ELSE 'claim value more than premium' 
# MAGIC   END 
# MAGIC   AS valid_amount
# MAGIC FROM temp2_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the location and speed
# MAGIC -- If the speed is so high

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check coverage of policy and the accident amount
# MAGIC CREATE TABLE temp4_claims_policy_accident AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  incident_severity="Total Loss" AND severity > 0.9 THEN  "Severity matches the report"
# MAGIC        WHEN  incident_severity="Major Damage" AND severity > 0.8 THEN  "Severity matches the report"
# MAGIC        WHEN  incident_severity="Minor Damage" AND severity > 0.7 THEN  "Severity matches the report"
# MAGIC        WHEN  incident_severity="Trivial Damage" AND severity > 0.4 THEN  "Severity matches the report"
# MAGIC        ELSE "Severity doesn't match"
# MAGIC   END 
# MAGIC   AS reported_severity_check
# MAGIC FROM temp3_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Add a new column for release funds.... if all the other generated rules are ok
# MAGIC 
# MAGIC CREATE TABLE claims_policy_accident_insights AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  reported_severity_check="Severity matches the report" and valid_amount="calim value in the range of premium" and valid_date="VALID" then "release funds"
# MAGIC 
# MAGIC        ELSE "claim needs more investigation" 
# MAGIC   END 
# MAGIC   AS release_funds
# MAGIC FROM temp4_claims_policy_accident)

# COMMAND ----------

# MAGIC %sql CREATE TABLE policy_claims_iot_insights AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  speed <= 45 and speed > 0 THEN  "Normal Speed"
# MAGIC        WHEN speed > 45 THEN  "High Speed"
# MAGIC        ELSE "Invalid speed"
# MAGIC   END 
# MAGIC   AS speed_check
# MAGIC FROM policy_claims_iot)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE policy_claims_iot_available_insights AS
# MAGIC (
# MAGIC SELECT *,
# MAGIC   CASE WHEN  speed <= 45 and speed > 0 THEN  "Normal Speed"
# MAGIC        WHEN speed > 45 THEN  "High Speed"
# MAGIC        ELSE "Invalid speed"
# MAGIC   END 
# MAGIC   AS speed_check
# MAGIC FROM policy_claims_iot_available)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM policy_claims_iot_available
