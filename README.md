# Azure Retail Lakehouse (ADLS Gen2 + Synapse Serverless + ADF)

This repo demonstrates a mini **lakehouse** on Azure using one retail CSV dataset.

- **WIN 1:** Query raw CSV in ADLS Gen2 using **Synapse Serverless OPENROWSET**  
- **WIN 2:** Create **Gold** Parquet external tables using **CETAS** (dim_date, dim_customer, dim_product, fact_sales)  
- **WIN 3:** Showcase **Azure Data Factory** orchestration (Get Metadata → If Condition → ForEach → Mapping Data Flow → Set Variable → Storage Events Trigger)

---

## Architecture (Medallion)


Storage account: `retaildatagit`  
Containers used: `bronze` (raw + gold outputs for this demo), optional `silver`  
Bronze file: `/bronze/retail_sales_dataset.csv`

---

## WIN 1 — Bronze (Query Raw with OPENROWSET)

- I connected Synapse Serverless to ADLS Gen2 via **Managed Identity**.
- I queried the CSV directly without loading to a DB table.

Script: [`sql/win1_openrowset.sql`](sql/win1_openrowset.sql)

**Screenshot (sample result):**  
![WIN1](images/win1_openrowset_result.png)

**What this shows:**  
- You can run **ad-hoc SQL** on files stored in Data Lake.
- Great for quick validation and exploration.

---

## WIN 2 — Gold (CETAS to Parquet External Tables)

I created four curated tables (as Parquet files) using **CETAS**:

- `dbo.dim_date` – date attributes (year, month, weekday, weekend flag)  
- `dbo.dim_customer` – customer id, gender, age, age_band  
- `dbo.dim_product` – product_category, price_per_unit  
- `dbo.fact_sales` – transaction_id, date_id, customer_id, product info, quantity, total_amount  

Script: [`sql/win2_gold_simple_star.sql`](sql/win2_gold_simple_star.sql)

**Screenshot (TOP 5 from each):**  
![WIN2](images/win2_tables_selects.png)

**Where files land:**  
- `/gold/simple/dim_date/`  
- `/gold/simple/dim_customer/`  
- `/gold/simple/dim_product/`  
- `/gold/simple/fact_sales/`  

**What this shows:**  
- How to convert raw CSV into **columnar Parquet**.  
- How to surface curated data as **external tables** for BI tools.

---

## WIN 3 — ADF Orchestration (Showcase Pipeline)

I built a single pipeline to demonstrate common ADF features. Order of activities:

1. **Get Metadata** – reads file info for `retail_sales_dataset.csv`  
2. **If Condition** – branches logic (e.g., weekend vs weekday)  
3. **ForEach** – loops through product categories (e.g., Clothing, Electronics, Groceries) and runs sub-tasks  
4. **Mapping Data Flow** – transformations used:
   - **Select** (choose useful columns)
   - **Filter** (remove rows with non-positive Total Amount)
   - **Conditional Split** (route rows by a rule)
   - **Sink** (write cleaned data to `/silver/retail_sales_clean/` as Parquet)
5. **Set Variable** – stores a final status message
6. **Storage Events Trigger** – fires the pipeline automatically when a new file lands in **bronze**  
   *(This repo is a showcase; the trigger/pipeline don’t need to be run.)*

**Screenshots:**  
- Pipeline canvas – ![Pipeline](images/win3_pipeline_canvas.png)  
- If Condition – ![If](images/win3_ifcondition.png)  
- ForEach – ![ForEach](images/win3_foreach.png)  
- Data Flow graph – ![DF](images/win3_dataflow_graph.png)  
- Storage Event Trigger – ![Trigger](images/win3_trigger_storage_events.png)

**ADF JSON for reference (browseable):**  
- Pipeline JSON – [`adf/pipelines/pl_retail_showcase.json`](adf/pipelines/pl_retail_showcase.json)  
- Data Flow JSON – [`adf/dataflows/df_retail_clean.json`](adf/dataflows/df_retail_clean.json)  
- Trigger JSON – [`adf/triggers/storage_event_trigger.json`](adf/triggers/storage_event_trigger.json)

**What this shows:**  
- Control flow (Get Metadata, If, ForEach, Set Variable)  
- Data flow (transformations and sink)  
- Event-driven orchestration (Storage Events Trigger)

---

## How to Reproduce (high level)

1. **ADLS Gen2**: create storage, container `bronze`, upload `retail_sales_dataset.csv`.  
2. **Access**: grant the Synapse workspace **Managed Identity** the role *Storage Blob Data Contributor* on the storage account.  
3. **Synapse Serverless**:
   - Run [`win1_openrowset.sql`](sql/win1_openrowset.sql) to validate raw file.
   - Run [`win2_gold_simple_star.sql`](sql/win2_gold_simple_star.sql) to create Gold Parquet tables.  
     *(If CETAS errors “folder exists,” delete the target `/gold/simple/...` folder(s) and re-run.)*
4. **ADF (optional showcase)**: import/open the JSON to view the pipeline and data flow design.

---

## Notes / Decisions

- I used **Parquet** for Gold because it’s columnar, compressed, and query-efficient.  
- I kept **WIN 2** simple by using natural keys; easy to extend to surrogate keys later.  
- **WIN 3** is designed to **show concepts** (branching, looping, transforms, triggers) without requiring execution.

---

## Contact

Built by ⟨Your Name⟩. Feedback welcome!
