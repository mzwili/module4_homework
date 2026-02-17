# Module 4 — Analytics Engineering with dbt (Homework 4)

## Overview

This project demonstrates building an analytics layer using **dbt + BigQuery** on the NYC Taxi dataset (2019–2020). The goal is to transform raw taxi trip data into clean, analytics‑ready tables and answer business questions using dimensional models.

We implement a standard analytics engineering workflow:

Raw Data → Staging Models → Intermediate Models → Fact & Dimension Models → Analytics Queries

---

## Tech Stack

* Google Cloud Storage (Data Lake)
* BigQuery (Data Warehouse)
* dbt Cloud (Transformations)
* SQL (Analytics Queries)
* Python (Data ingestion)

---

## Dataset

We load the following datasets into BigQuery:

| Dataset                 | Years       |
| ----------------------- | ----------- |
| Yellow Taxi             | 2019 – 2020 |
| Green Taxi              | 2019 – 2020 |
| FHV (For‑Hire Vehicles) | 2019        |

Data source: DataTalksClub NYC TLC dataset release files.

---

## Project Structure

```
04-analytics-engineering/
│
├── ingestion/
│   ├── load_to_gcs.py
│   └── data/                # ignored in git
│
├── models/
│   ├── staging/
│   │   ├── stg_green_tripdata.sql
│   │   ├── stg_yellow_tripdata.sql
│   │   └── stg_fhv_tripdata.sql
│   │
│   ├── intermediate/
│   │   └── int_trips_unioned.sql
│   │
│   └── marts/
│       ├── fct_trips.sql
│       ├── dim_zones.sql
│       └── fct_monthly_zone_revenue.sql
│
└── README.md
```

---

## Setup Instructions

### 1. Load data to GCS

```bash
python ingestion/load_to_gcs.py
```

### 2. Create BigQuery External Tables

Create two external tables pointing to GCS files:

* yellow_tripdata
* green_tripdata

### 3. Run dbt models

```bash
dbt build --target prod
```

This will:

* Run staging models
* Create intermediate models
* Create fact and dimension tables
* Run data quality tests

---

## dbt Models Explained

### Staging

Cleans raw data and standardizes column names

* stg_green_tripdata
* stg_yellow_tripdata
* stg_fhv_tripdata

### Intermediate

Combines datasets into a unified trips table

* int_trips_unioned

### Marts

Business‑ready analytics tables

* fct_trips → All taxi trips
* dim_zones → Taxi zones lookup
* fct_monthly_zone_revenue → Monthly revenue by zone

---

## Homework Questions & Answers


### Q3 — Count of fct_monthly_zone_revenue

SELECT 
    COUNT(*) AS total_records 
FROM {{ ref('fct_monthly_zone_revenue') }}


### Q4 — Highest Revenue Zone (Green 2020)

SELECT 
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue_2020
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE 
    service_type = 'Green'
    AND EXTRACT(YEAR FROM month) = 2020
GROUP BY 1
ORDER BY total_revenue_2020 DESC
LIMIT 1;


### Q5 — Green Taxi Trips (Oct 2019)

SELECT 
    count(*) as total_trips
FROM {{ ref('stg_green_tripdata') }}
WHERE 
    lpep_pickup_datetime >= '2019-10-01' 
    AND lpep_pickup_datetime < '2019-11-01'


### Q6 — FHV Staging Count

SELECT 
    COUNT(*) AS total_records 
FROM {{ ref('stg_fhv_tripdata') }}


---
