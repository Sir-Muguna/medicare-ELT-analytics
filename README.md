# The Data Ingestion Story: From Multiple Sources to Snowflake Raw

![Architecture](/docs/ELT_architecture.png)

## Chapter 1: Laying the Foundation

I started by building my data warehouse foundation in **Snowflake**. I created dedicated entities—`HEALTHCARE_RAW_DB` database, `RAW` schema, and `AIRBYTE_HEALTHCARE` role—to ensure clean separation and cost tracking for ingestion.

![Sources](/docs/sources_airbyte.png)

## Chapter 2: The First Connection - Google Sheets

My journey began with the simplest sources: **Google Sheets**. I connected two critical reference files:
- **Hospital General Info**: Master data about healthcare facilities
- **ICD-11 Codes**: Medical classification codes

In Airbyte Cloud, I used the Google Sheets connector, pasted the spreadsheet URLs, and watched as the structured tabular data flowed seamlessly into Snowflake's raw layer. Two tables were born: `_AIRBYTE_RAW_GSHEETS_HOSPITAL_INFO` and `_AIRBYTE_RAW_GSHEETS_ICD_CODES`.

## Chapter 3: Bridging the Local Gap with Ngrok

Next, I tackled my **local PostgreSQL database** containing outpatient charge data from 2011-2013. Since Airbyte Cloud couldn't access my local machine directly, I used **ngrok** to create a secure tunnel. 

I exposed port 5432 to the internet with a simple command: `ngrok tcp 5432`. The magic happened when I took the generated public URL and configured it in Airbyte's PostgreSQL source. Suddenly, my local tables (`outpatient_charges_2011`, `2012`, `2013`) were replicating to Snowflake as if they were in the cloud all along.

## Chapter 4: Taming Unstructured JSON from Google Drive

The most challenging part was the **JSON files** in Google Drive—`inpatient_2011.json`, `2012.json`, `2013.json`. These contained arrays of unstructured patient data. 

The breakthrough came when I used Airbyte's **User Schema** feature. I defined the structure upfront, telling Airbyte how to interpret each field. The JSONL format transformed those nested arrays into clean, typed columns in `_AIRBYTE_RAW_GDRIVE_INPATIENT`.

## Chapter 5: The Rest API Connection

Finally, I connected to the **Medicare API**, this API provides Medicare Part D prescription drug data showing what medications physicians prescribed, including costs, volumes, and geographic patterns for 2013.. The HTTP connector in Airbyte made this surprisingly simple—just the API endpoint and authentication details. I set it to incremental sync mode, ensuring only new data would transfer each month.

## The Grand Finale: Unified Raw Data

Within hours, I had built a robust pipeline ingesting from four disparate sources into a single **Snowflake RAW schema**. The data journey was complete:

- ✅ **Google Sheets** → Reference tables
- ✅ **PostgreSQL (via Ngrok)** → Historical outpatient data  
- ✅ **Google Drive JSON** → Inpatient records
- ✅ **Medicare API** → Physician prescribing patterns, drug costs & geographic data for 2013

![Snowflake_RAW](/docs/snowflake_db_raw.png)

All landing neatly in structured tables, ready for the next transformation phase with dbt. The raw healthcare data ecosystem was alive!

---
*Next Chapter: Transforming raw data into analytics-ready models with dbt Cloud...*
# Chapter 6: The Transformation Journey with dbt Cloud

## Building the Staging Layer Foundation
With all raw data securely in Snowflake, I began the transformation phase using dbt Cloud.  
**Goal**: create a clean, consistent staging layer that would serve as the foundation for analytical models.

---

## The Staging Layer Architecture
I built 5 comprehensive staging models that transformed **81,710 records** from disparate sources into analytics-ready datasets:

### 🏥 stg_hospitals (5,336 records)
- **Source**: Hospital general information from Google Sheets  
- **Transformations**: Dropped 4 high-NULL columns, converted emergency services to boolean, standardized 20+ quality metrics  
- **Key Insight**: Perfect geographic coverage across 56 states with hospital ratings (1-5 scale)

---

### 💊 stg_medicare_data (931 records)
- **Source**: Medicare Part D prescription drug API  
- **Transformations**: Converted string numerics to proper types, standardized boolean flags, created composite drug keys  
- **Key Insight**: Physician prescribing patterns with cost and volume data for 2013  

---

### 🏥 stg_inpatient_visits (30,000 records)
- **Source**: 3 years of inpatient data (2011-2013) oogle Drive JSON  
- **Transformations**: Combined years with explicit `service_year` column, cleaned provider data, standardized financial metrics  
- **Key Insight**: 100% data completeness with discharge volumes from 11–1,328 per provider  

---

### 🩺 stg_outpatient_visits (9,315 records)
- **Source**: 3 years of outpatient data (2011-2013) from PostgreSQL  
- **Transformations**: Year integration, text cleaning, data type consistency  
- **Key Insight**: Perfect data quality maintained across all 14 columns  

---

### 📋 stg_icd_11_codes (36,128 records)
- **Source**: Disease classification codes from Google Drive JSON  
- **Transformations**: Strategic column dropping (>65% NULL threshold), boolean conversions, hierarchy preservation  
- **Key Insight**: Comprehensive medical terminology with 8-level classification hierarchy  

---

## Data Quality Excellence

Through rigorous Exploratory Data Analysis (EDA) using Snowflake worksheets, I established:

- ✅ 100% completeness for inpatient and outpatient datasets  
- ✅ Strategic NULL handling with evidence-based column retention  
- ✅ Type safety with proper casting to **INTEGER, FLOAT, and BOOLEAN**  
- ✅ Consistent naming conventions across all models  

---

# The Staging Layer Triumph

![Snowflake_STG](/docs/snowflake_db_stg.png)

I successfully transformed raw, disparate healthcare data into a unified staging layer.  
The data journey evolved from:

The foundation is now set for the most exciting phase: **building analytical models that will reveal actionable healthcare insights.**

---

## Next Chapter...
**Building the analytical mart layer — transforming clean data into business intelligence...**

