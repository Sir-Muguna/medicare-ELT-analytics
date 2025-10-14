
---

````markdown
# 🏥 Healthcare Data Platform — End-to-End Ingestion & Transformation Pipeline

## 📘 Overview
This project demonstrates an **end-to-end ELT pipeline** that ingests, transforms, and models healthcare data from multiple sources into **Snowflake**, powered by **Airbyte** for ingestion and **dbt Cloud** for transformation.

The pipeline integrates structured, semi-structured, and API-based data sources to deliver an analytics-ready healthcare data mart.

---

## 🧱 Architecture

![ELT Architecture](/docs/ELT_architecture.png)

**Components**
- **Airbyte Cloud** → Data ingestion from 4 unique sources  
- **Snowflake** → Centralized data warehouse  
- **dbt Cloud** → Transformation, testing, and modeling  
- **Snowflake Worksheets** → EDA and validation

---

## ⚙️ Data Ingestion (Airbyte → Snowflake RAW)

### 1. Google Sheets (Reference Data)
**Sources**
- `Hospital General Info`
- `ICD-10 Codes`
- `ICD-11 Codes`

**Connector**: Google Sheets → Snowflake  
**Destination Tables**
- `_AIRBYTE_RAW_GSHEETS_HOSPITAL_INFO`
- `_AIRBYTE_RAW_GSHEETS_ICD_CODES`

---

### 2. PostgreSQL (Historical Data via Ngrok)
**Description**: Outpatient charge data (2011–2013) from local PostgreSQL database  
**Setup**
```bash
ngrok tcp 5432
````

**Connector**: PostgreSQL → Snowflake via public ngrok endpoint
**Destination Tables**

* `_AIRBYTE_RAW_POSTGRES_OUTPATIENT_2011`
* `_AIRBYTE_RAW_POSTGRES_OUTPATIENT_2012`
* `_AIRBYTE_RAW_POSTGRES_OUTPATIENT_2013`

---

### 3. Google Drive (Unstructured JSON)

**Description**: Inpatient discharge data (`inpatient_2011.json`, `2012.json`, `2013.json`)
**Connector**: Google Drive → Snowflake
**Approach**

* Used **User Schema Definition** feature to predefine structure for JSON arrays
* Converted to JSONL and loaded into `_AIRBYTE_RAW_GDRIVE_INPATIENT`

---

### 4. Medicare REST API

**Description**: Medicare Part D prescription drug data (2013)
**Connector**: HTTP API → Snowflake
**Configuration**

* Incremental sync mode
* API authentication via token
  **Destination Table**
* `_AIRBYTE_RAW_API_MEDICARE`

---

### ✅ Unified Raw Layer

All raw datasets land in `HEALTHCARE_RAW_DB.RAW` within Snowflake.

| Source        | Connector          | Table Prefix             | Data Type       |
| ------------- | ------------------ | ------------------------ | --------------- |
| Google Sheets | Sheets → Snowflake | `_AIRBYTE_RAW_GSHEETS_`  | Structured      |
| PostgreSQL    | Ngrok Tunnel       | `_AIRBYTE_RAW_POSTGRES_` | Relational      |
| Google Drive  | JSON               | `_AIRBYTE_RAW_GDRIVE_`   | Semi-Structured |
| REST API      | HTTP               | `_AIRBYTE_RAW_API_`      | Incremental     |

![Snowflake RAW Schema](/docs/snowflake_db_raw.png)

---

## 🧩 Transformation (dbt Cloud → Staging & Marts)

### Project Goals

* Clean and standardize all ingested data
* Establish a **reliable staging layer**
* Build **business-ready marts** for analytics

---

## 🧮 Staging Layer (Snowflake + dbt Cloud)

### Summary

| Model                     | Source            | Records | Description                                                 |
| -----------------------   | ----------------- | ------- | ----------------------------------------------------------- |
| `stg_hospital_general_info| Google Sheets     | 5,336   | Cleansed hospital metadata, standardized quality metrics    |
| `stg_medicare_data`       | Medicare API      | 931     | Normalized drug cost & volume fields                        |
| `stg_inpatient_visits`    | Google Drive JSON | 30,000  | Combined yearly inpatient data with standardized financials |
| `stg_outpatient_visits`   | PostgreSQL        | 9,315   | Unified 3-year outpatient data                              |
| `stg_icd_10_codes`        | Google Sheets     | 1,871   | Structured ICD-10 hierarchy                                 |
| `stg_icd_11_codes`        | Google Sheets     | 36,128  | Structured ICD-11 hierarchy                                 |

**Quality Checks**

* 100% completeness for patient data
* Consistent naming (snake_case)
* Correct numeric & boolean data types
* Strategic NULL handling based on column thresholds

![Snowflake STG Schema](/docs/snowflake_db_stg.png)

---

## 🧠 Analytics Mart Layer (dbt Cloud)

### Overview

6 production-ready models built using **dimensional modeling** principles (star schema).
All marts are **materialized as tables** for high-performance analytics.

---

### 1. `fact_payment_analysis`

**Purpose:** Unified Medicare payment comparison (inpatient + outpatient)
**Features**

* Provider-level ranking
* Payment-to-charge efficiency
* Multi-year trend analysis

---

### 2. `dim_providers`

**Purpose:** Single provider reference table
**Features**

* Geographic hierarchy (State → City → Zip)
* Emergency service flags
* Quality rating metrics

---

### 3. `dim_diagnosis_codes`

**Purpose:** ICD-10 and ICD-11 integrated dimension
**Features**

* Hierarchical disease categorization
* Clinical pattern mapping

---

### 4. `fact_outpatient_services`

**Purpose:** Analyze APC service charge trends
**Features**

* Provider–APC–Year metrics
* Efficiency ratio & service categorization

---

### 5. `fact_inpatient_services`

**Purpose:** Inpatient discharge & payment analysis
**Features**

* Provider–ICD–Year metrics
* Clinical prevalence trends by state

---

### 6. `fact_medicare_claims`

**Purpose:** Prescription drug claims aggregation
**Features**

* Drug–Year–Cost rollup
* Therapeutic category analysis

---

## 🏗️ Architecture Standards

| Area              | Standard                                            |
| ----------------- | --------------------------------------------------- |
| **Modeling**      | Star schema with conformed dimensions               |
| **Naming**        | snake_case for columns & models                     |
| **Data Quality**  | dbt tests (`unique`, `not_null`, `accepted_values`) |
| **Performance**   | Table materialization + Snowflake clustering        |
| **Documentation** | YAML schema + description fields for all models     |

---

## 💼 Business Use Cases

| Requirement                  | Model                                             | Description                       |
| ---------------------------- | ------------------------------------------------- | --------------------------------- |
| Payment ranking per provider | `fact_payment_analysis`                           | Medicare performance benchmarking |
| Service trend analysis       | `fact_outpatient_services`                        | Time-series charge analytics      |
| Geographic affordability     | `dim_providers`                                   | State-level provider comparison   |
| Diagnosis pattern analysis   | `fact_inpatient_services` + `dim_diagnosis_codes` | Top ICD-11 diagnosis trends       |

---

## 📊 Expected Outcomes

### Healthcare Administrators

* Provider benchmarking & regional performance tracking
* Gap analysis in hospital service coverage

### Clinical Teams

* Diagnosis pattern insights by geography
* Data-backed resource planning

### Financial Analysts

* Cost efficiency and payment ratio analysis
* Forecasting based on multi-year trends

### Public Health Analysts

* Drug utilization pattern tracking
* Elderly population medication cost analysis

---

## 🚀 Deployment Summary

| Layer   | Tool      | Materialization | Status     |
| ------- | --------- | --------------- | ---------- |
| RAW     | Airbyte   | Tables          | ✅ Complete |
| STAGING | dbt Cloud | Views           | ✅ Complete |
| MARTS   | dbt Cloud | Tables          | ✅ Complete |

**Final Output:**
A unified, analytics-ready Snowflake data warehouse supporting healthcare insights across financial, clinical, and pharmaceutical domains.

---

## 🧭 Next Steps

* Integrate Looker Studio / Power BI for visualization
* Add CI/CD automation for dbt jobs via GitHub Actions
* Extend API ingestion to include Medicare datasets (2014–2015)

---

**Author:** Ronald Muguna
**Tech Stack:** Airbyte | Snowflake | dbt Cloud | PostgreSQL | REST API | Ngrok
**Status:** ✅ Production-Ready

```

---

