# ✈️ Real-Time Flight Data Pipeline with Databricks & Delta Live Tables (DLT)

This project is a fully functional **real-time data engineering pipeline** built using **Databricks**, **Delta Lake**, and **Delta Live Tables (DLT)** to process, transform, and manage streaming flight data using the Medallion Architecture (Bronze → Silver → Gold).

---

## 📌 Project Summary

- **Ingestion**: Real-time ingestion of raw flight-related data using **Databricks Autoloader**
- **Transformation**: Used **Delta Live Tables (DLT)** for scalable, modular, and continuous transformations
- **Medallion Architecture**: Implemented complete data flow through Bronze, Silver, and Gold layers
- **Deployment**: Leveraged serverless compute in Databricks for pipeline execution

---

## 🧠 Key Features

| Layer   | Purpose                                           | Tech Used               |
|---------|---------------------------------------------------|-------------------------|
| Bronze  | Raw streaming data ingestion                      | Autoloader + Delta      |
| Silver  | Cleansed, deduplicated, and enriched data         | DLT + PySpark           |
| Gold    | Business-ready aggregated tables for analytics    | PySpark SQL (Gold View) |

---

## 🧱 Tech Stack

- **Databricks**
- **Delta Live Tables (DLT)**
- **Delta Lake**
- **PySpark**
- **Streaming Structured APIs**
- **Medallion Architecture**
- **GitHub (Version Control)**

---

## 🛠️ Project Structure


---

## 🔁 Pipeline Flow (Architecture)

```text
       ┌────────────┐
       │  Autoloader│  (Streaming ingestion from /bronze/)
       └────┬───────┘
            ▼
   ┌────────────────┐
   │  Bronze Layer  │ - Raw Data (Delta Table)
   └────┬────┬──────┘
        ▼    ▼
 ┌────────┐ ┌────────────┐
 │ Silver │ │ Transform  │ - Cleaned, joined (flights, passengers, bookings)
 └────┬───┘ └──────┬─────┘
      ▼            ▼
      └────────┬───┘
               ▼
        ┌───────────────┐
        │ Gold Layer     │ - Business insights / final analytics
        └───────────────┘


💡 Pipeline Configuration
Recreated config: DLT_pipeline_config.json

Key Details:

Name: SILVER_DLT_PIPELINE

Compute: Serverless (1 worker)

Storage: /Volumes/workspace/bronze/bronzevolume/bookings/data/

Target: silver schema

Catalog: workspace

Notebook: dltpipeline.py

Mode: Continuous, Photon enabled


🚀 How to Run This Project
Clone the repo or import into Databricks

Import the notebook: notebooks/dltpipeline.py

Open Jobs & Pipelines → Create Pipeline → Configure using DLT_pipeline_config.json

Run the pipeline — results will stream and update continuously


👨‍💻 Author
Arnab Dutta
Aspiring Data Engineer | Real-time Data Pipelines | Databricks | Azure
📧 arnabard46@gmail.com
🔗 https://www.linkedin.com/in/arnab-dutta-313272203/


Let me know if you also want:
- A custom diagram
- Gold-layer transformations breakdown
- Resume bullet points based on this project

I'm here to help package it fully recruiter-ready!

