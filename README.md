# USPS Service Performance Insights Across Rural America

## Team Nexus Ninjas | Challenge X | George Mason University
### First Place Winner — Challenge X Hackathon 2026

## Project Overview
An automated end-to-end data pipeline that ingests, processes, and visualizes 
USPS service performance data to identify rural vs urban mail delivery disparities 
across America. Built for the USPS Office of Inspector General.

## Live Dashboard
The dashboard is currently paused due to GCP billing constraints.
To view the dashboard:
- Watch the demo video included in the presentation
- Or activate using the setup instructions below

## Key Findings
- Rural On-Time Rate: 81.9% vs Urban: 81.3% (both below 89% FY26 target)
- Rural delivers faster — 3.09 days vs 3.28 days average
- Worst district: Indiana rural at 69.4% on time
- Bound Printed Matter rural areas as low as 32.9% on time
- December 2025 holiday dip — both rural and urban dropped to 76%
- Rural recovering faster — reaching 87.2% by March 2026 vs urban 83.6%

## Architecture
USPS Server → Laptop Relay → GCP Cloud Storage → GCP Dataproc (Spark) → Streamlit Dashboard

## Tech Stack
- Python — automated data ingestion pipeline
- Apache Spark — distributed processing of 1B+ rows
- Google Cloud Platform — Dataproc + Cloud Storage
- Streamlit + Plotly — interactive dashboard
- HRSA FORHP — rural/urban ZIP classification

## Scripts
- `usps_downloader.py` — downloads USPS files from spm.usps.com, uploads to GCP
- `usps_both_analysis.py` — main Spark analysis (sending + receiving performance)
- `usps_trend_analysis.py` — monthly trend analysis over time
- `usps_clean_analysis.py` — full Spark analysis from scratch (backup)
- `usps_dashboard.py` — Streamlit interactive dashboard

## GCP Setup (to reactivate)
1. Enable billing on GCP project: project-4be3a115-f3f9-404e-8ba
2. Create Dataproc cluster:
```bash
gcloud dataproc clusters create usps-cluster \
    --region=us-central1 \
    --master-machine-type=e2-standard-4 \
    --num-workers=2 \
    --worker-machine-type=e2-standard-4 \
    --image-version=2.1-debian11
```
3. Run analysis: `gcloud dataproc jobs submit pyspark usps_both_analysis.py`
4. Run dashboard: `streamlit run usps_dashboard.py`

## Data Sources
- USPS Service Performance Dashboard: spm.usps.com
- HRSA FORHP Rural/Urban ZIP Classification: hrsa.gov

## Team
- Hrishitha Reddy Likki — Data Pipeline Lead & Analytics Engineer
- Mohan Krishna Vallabhaneni — Data Analyst
- Arjun Marimuthu Senthil Kumar — Data Analyst
