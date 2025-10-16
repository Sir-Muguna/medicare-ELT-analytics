# 🏥 Production-Grade Healthcare Data Platform: Airbyte + dbt + Snowflake ELT Pipeline

## 📘 Project Overview
This repository details the implementation of a robust, **end-to-end ELT (Extract-Load-Transform) pipeline** designed to consolidate disparate healthcare data sources into a unified, analytics-ready **Snowflake** data mart.

The solution leverages **Airbyte Cloud** for reliable, multi-source ingestion and **dbt Cloud** for efficient transformation, modeling, and data quality enforcement. The resulting architecture supports critical financial, clinical, and administrative use cases.

---

## 🧱 Architecture
The system follows a modern Data Warehouse paradigm, moving data from diverse sources through Raw and Staging layers into a final Analytics Mart.

![ELT Architecture](/docs/ELT_architecture.png)

**Core Components**
- **Airbyte Cloud** 📤 → Data ingestion from 4 unique, heterogenous sources (APIs, DBs, Files).
- **Snowflake** ❄️ → Centralized, scalable cloud data warehouse for all layers.
- **dbt Cloud** ⚙️ → Orchestration, transformation (SQL), testing, and dimensional modeling.
- **Ngrok** 🔗 → Secure tunneling for local PostgreSQL database exposure to Airbyte.

---

## ⚙️ Data Ingestion Layer (Airbyte → Snowflake RAW)
Data is ingested from four distinct source types into the `HEALTHCARE_RAW_DB.RAW` schema in Snowflake, maintaining the original structure.

### 1. Google Sheets (Reference & Code Data)
Reference datasets critical for lookups and standardization.
**Sources:** `Hospital General Info`, `ICD-10 Codes`, `ICD-11 Codes`
**Connector:** Google Sheets → Snowflake

### 2. PostgreSQL (Historical Relational Data)
Handling historical outpatient charge data (2011–2013) from a local environment.
**Setup:** Securely exposed via `ngrok tcp 5432`
**Connector:** PostgreSQL → Snowflake via public ngrok endpoint

### 3. Google Drive (Unstructured JSON)
Managing complex, semi-structured JSON files (Inpatient discharge data).
**Approach:** Utilized Airbyte's **User Schema Definition** feature to predefine structure and convert JSON arrays into a loadable format (`JSONL`).

### 4. Medicare REST API (Incremental Claims Data)
Ingesting high-volume, dynamic prescription drug data.
**Configuration:** Implemented **Incremental Sync Mode** and API token authentication.

### ✅ Unified Raw Layer Structure
| Source | Connector | Table Prefix | Data Type |
| :--- | :--- | :--- | :--- |
| Google Sheets | Sheets → Snowflake | `_AIRBYTE_RAW_GSHEETS_` | **Structured** (Reference) |
| PostgreSQL | Ngrok Tunnel | `_AIRBYTE_RAW_POSTGRES_` | **Relational** (Historical) |
| Google Drive | JSON | `_AIRBYTE_RAW_GDRIVE_` | **Semi-Structured** (Files) |
| REST API | HTTP | `_AIRBYTE_RAW_API_` | **Incremental** (API) |

![Snowflake RAW Schema](/docs/snowflake_db_raw.png)

---

## 🧩 Transformation Layer (dbt Cloud)

### Project Goals
- **Clean & Standardize:** Apply initial cleansing and type casting to raw data.
- **Reliable Staging:** Create deterministic, foundational views (`stg_`) for downstream models.
- **Analytics Marts:** Build high-performance **Star Schema** models for BI consumption.

---

## 🧮 Staging Layer (`HEALTHCARE_STG_DB.STAGING`)
The staging layer materializes as **Views** for efficiency, standardizing all columns to `snake_case` and ensuring data quality.

| Model | Source | Records | Key Transformations |
| :--- | :--- | :--- | :--- |
| `stg_hospital_general_info` | Google Sheets | 5,336 | Cleansed hospital metadata, standardized quality metrics. |
| `stg_medicare_data` | Medicare API | 931 | Normalized drug cost & volume fields for consistency. |
| `stg_inpatient_visits` | Google Drive JSON | 30,000 | Unified yearly inpatient data; standardized financials & key IDs. |
| `stg_outpatient_visits` | PostgreSQL | 9,315 | Unified 3-year outpatient data into a single staging table. |
| `stg_icd_codes` | Google Sheets | 38,000+ | Combined/Structured ICD-10 & ICD-11 hierarchies. |

**Data Quality Checks (dbt Tests)**
- **100% completeness** enforced on critical patient/claim keys.
- **Consistent data types** (e.g., correct numerics, booleans).
- **Strategic NULL handling** applied where data sparsity was an issue.

![Snowflake STG Schema](/docs/snowflake_db_stg.png)

---

## 🧠 Analytics Mart Layer (`HEALTHCARE_MARTS_DB.ANALYTICS`)
The core of the platform: 6 production-optimized models built on **Dimensional Modeling** principles. All marts are materialized as **Tables** for sub-second query performance.

### **Dimensional Models (Reference)**
1.  **`dim_providers`** 🧍: Single source of truth for all provider metadata, including geographic and quality metrics.
2.  **`dim_diagnosis_codes`** 🧬: Integrated ICD-10 and ICD-11 codes with hierarchical categorization.

### **Fact Models (Metrics & Events)**
3.  **`fact_payment_analysis`** 💰: Unified fact table for cross-platform (inpatient/outpatient) Medicare payment comparisons and efficiency analysis.
4.  **`fact_outpatient_services`** 🏥: Focuses on APC service charge trends, provider-level metrics, and service categorization.
5.  **`fact_inpatient_services`** 🛌: Detailed discharge and payment analysis, enabling clinical prevalence trends by state.
6.  **`fact_medicare_claims`** 💊: Aggregated prescription drug claims data for cost and therapeutic category analysis.

![Snowflake MARTS Schema](/docs/snowflake_db_marts.png)

---

## 🏗️ Architecture Standards
| Area | Standard |
| :--- | :--- |
| **Modeling** | **Star Schema** with conformed dimensions (e.g., `dim_providers` joins all facts). |
| **Naming** | Strict `snake_case` across all columns and models (`stg_`, `dim_`, `fact_` prefixes). |
| **Data Quality** | Comprehensive **dbt tests** (`unique`, `not_null`, `accepted_values`) on all primary keys and critical business columns. |
| **Performance** | Table materialization for all marts + strategic **Snowflake clustering** on high-cardinality keys. |
| **Documentation** | Full **YAML schema** documentation and description fields for all models, sources, and columns. |

![Star Schema Architecture](/docs/medicare_mart_layer_ERD.png)

---

## 💼 Business Use Cases & Value
The final mart layer directly supports key business and clinical needs:

| Requirement | Supporting Model(s) | Business Value |
| :--- | :--- | :--- |
| **Provider Performance Benchmarking** | `fact_payment_analysis` + `dim_providers` | Rank providers on cost-efficiency and quality ratings. |
| **Clinical Diagnosis Pattern Tracking** | `fact_inpatient_services` + `dim_diagnosis_codes` | Identify top ICD-11 diagnosis trends by region for resource planning. |
| **Geographic Affordability Analysis** | `dim_providers` | State-level comparison of charge vs. payment ratios. |
| **Drug Utilization & Cost Forecasting** | `fact_medicare_claims` | Analyze therapeutic category spend and track multi-year trends. |

---

## 🚀 Deployment Summary
The pipeline is fully operational, providing a governed and tested analytics environment.

| Layer | Tool | Materialization | Status |
| :--- | :--- | :--- | :--- |
| **RAW** | Airbyte | Tables | ✅ Complete |
| **STAGING** | dbt Cloud | Views | ✅ Complete |
| **MARTS** | dbt Cloud | Tables | ✅ Complete |

---

## 🧭 Future Roadmap
- Integrate **Looker Studio / Power BI** for final visualization dashboards.
- Implement full **CI/CD** automation for dbt jobs via GitHub Actions.
- Extend API ingestion to include additional Medicare datasets (e.g., 2014–2015).

---

**Author:** Ronald Muguna
**Tech Stack:** Airbyte | Snowflake | dbt Cloud | PostgreSQL | REST API | Ngrok
**Status:** ✅ Production-Ready