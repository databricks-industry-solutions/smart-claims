<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

* Domain: Insurance 
* Challenge: How to improve the Claims Management process for faster claims settlement, lower claims processing costs and quicker identification of possible fraud.
* Smart Claims: A Databricks Solution Accelerator that uses the Lakehouse paradigm to automate certain components of this process that aids human investigation *
* What
  * There is a lot of customer churn in insurance companies. 
  * How can customer loyalty & retention be  improved?
  * Processing Claims is time consuming
  * How can funds and resources be released in a timely manner to deserving parties?
  * Fraudulent transactions erodes profit margins
  * How can suspicious activities be flagged for  further investigation?
* Why
  * Faster approvals, Lower Operating expenses
  * Detect & Prevent fraudulent scenarios, Lower Leakage ratio
  * Improve customer satisfaction, Lower Loss ratio
How
* Claim Automation
  * What aspects of the claims processing pipeline can be automated
  * Augmenting Info to claims data to aid Investigation - Recommend Next Best Action
  * Explainability for the human workflow
  * Claims Role (Adjustor, skill set, tenure time - who should take it based on claim characteristics)


___
<anindita.mahapatra@databricks.com>
<marzi.rasooli@databricks.com>
<sara.slone@databricks.com>
___

# Smart Claims Reference Architecture & Data Flow
<img src="./images/smart_claims_process.png" width="10%" height="10%">
/*:
1. Policy data ingestion 
2. Claims and telematics data ingestion 
3. Ingest all data sources to the cloud storage
4. Incrementally Load Raw data to Delta Bronze table
5. Transform and Manipulate data
6. Model scoring (and model training in the training pipeline)
7. Load predictions to a gold table and perform aggregations
8. Dashboard visualization
9. Feed the results back to the operational system
10. Claims routing based on decision
*/

___

# Datasets
<img src="./images/datasets.png" width="10%" height="10%">
/*:
1. Policy data ingestion 
2. Claims and telematics data ingestion 
3. Ingest all data sources to the cloud storage
4. Incrementally Load Raw data to Delta Bronze table
*/
___

# Domain Model
<img src="./images/domain_model.png" width="10%" height="10%">

# Rule Engine
<img src="./images/rule_engine.png" width="10%" height="10%">

# Workflow
<img src="./images/workflow.png" width="10%" height="10%">
<img src="./images/medallion_architecture.png" width="10%" height="10%">
___
# Dashboards
<img src="./images/summary_dashboard.png" width="10%" height="10%">
___
&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

