<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

* <b>Domain </b>: Insurance 
* <b>Challenge </b>: 
 * Insurance companies have to constantly innovate to beat competition
 * Customer Retention/Loyalty can be challenge as people are always shopping for more competitive rates, leading to ccustomer churn
 * Fraudulent transactions erodes profit margins 
 * How to improve the Claims Management process for faster claims settlement, lower claims processing costs and quicker identification of possible fraud.
* <b><span style="color: red"> Smart Claims </span></b>: 
* A Databricks Solution Accelerator that uses the Lakehouse paradigm to automate certain components of this process that aids human investigation *
___

# Details
* <b>What</b>
  * How to manage costs to offer. lower premiums to remain competitive & yet profitable?
  * How to prevent customer churn in insurance companies? 
  * How can customer loyalty & retention be  improved?
  * How to improve Processing Claims which can be very time consuming at times?
  * How can funds and resources be released in a timely manner to deserving parties?
  * How can suspicious activities be flagged for  further investigation?
* <b>Why</b>
  * Faster approvals, Lower Operating expenses
  * Detect & Prevent fraudulent scenarios, Lower Leakage ratio
  * Improve customer satisfaction, Lower Loss ratio
* <b>How: Claims Automation</b>
  * What aspects of the claims processing pipeline can be automated
  * Augmenting Info to claims data to aid Investigation - Recommend Next Best Action
  * Explainability for the human workflow
  * Claims Role (Adjustor, skill set, tenure time - who should take it based on claim characteristics)


# Smart Claims Reference Architecture & Data Flow
<img src="./resource/images/smart_claims_process.png" width="70%" height="70%">

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

___

# Datasets
<img src="./resource/images/datasets.png" width="60%" height="60%">

1. Policy data ingestion 
2. Claims and telematics data ingestion 
3. Ingest all data sources to the cloud storage
4. Incrementally Load Raw data to Delta Bronze table

___

# Domain Model
<img src="./resource/images/domain_model.png" width="80%" height="80%">

# Rule Engine
<img src="./resource/images/rule_engine.png" width="50%" height="50%">

# Workflow
<img src="./resource/images/workflow.png" width="60%" height="60%">
<img src="./resource/images/medallion_architecture_dlt.png" width="60%" height="60%">
___
# Dashboards
<img src="./resource/images/summary_dashboard.png" width="60%" height="60%">
___
<anindita.mahapatra@databricks.com> <br>
<marzi.rasooli@databricks.com> <br>
<sara.slone@databricks.com> <br>
___
&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

