# usps-service-performance
# USPS Service Performance Insights Across Rural America

## Team Nexus Ninjas | Challenge X | George Mason University

## Project Overview
An automated end-to-end data pipeline that ingests, processes, and visualizes 
USPS service performance data to identify rural vs urban mail delivery disparities 
across America.

## Key Findings
- Rural On-Time Rate: 81.9% vs Urban: 81.3% (both below 89% FY26 target)
- Rural delivers faster — 3.09 days vs 3.28 days average
- Worst district: Indiana rural at 69.4% on time
- Bound Printed Matter rural areas as low as 44% on time

## Architecture
USPS Server → Laptop Relay → GCP Cloud Storage → GCP Dataproc (Spark) → Streamlit Dashboard

## Tech Stack
- Python — automated data ingestion pipeline
- Apache Spark — distributed processing of 1B+ rows
- Google Cloud Platform — Dataproc + Cloud Storage
- Streamlit + Plotly — interactive dashboard

## Scripts
- 'usps_downloader.py' — downloads USPS files, uploads to GCP
- 'usps_incremental_analysis.py' — Spark analysis, processes new files only
- 'usps_clean_analysis.py' — full Spark analysis from scratch
- 'usps_dashboard.py' — Streamlit interactive dashboard

## Data Sources
- USPS Service Performance Dashboard: spm.usps.com
- HRSA FORHP Rural/Urban ZIP Classification: hrsa.gov

## Team
- Hrishitha Reddy Likki — Data Pipeline Lead & Analytics Engineer
- Mohan Krishna Vallabhaneni — Data Analyst
- Arjun Marimuthu Senthil Kumar — Data Analyst
