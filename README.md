# NYC For-Hire Vehicle (FHV) Data Pipeline & Analytics

A Databricks end-to-end pipeline processing 3 months of raw NYC For-Hire Vehicle trip records. 
The goal was to build a reliable Medallion Architecture (Bronze -> Silver -> Gold), run schema enforcement and data quality checks, model the data into a Star Schema, and answer key operational questions for city transport planning.

---

## Architecture
[Raw Parquet Files + Zone CSV]
│
▼
┌──────────────────┐
│   BRONZE LAYER   │  Raw Delta copy (Immutable staging)
└────────┬─────────┘
│
▼
┌──────────────────┐
│   SILVER LAYER   │  PySpark cleaning, schema casting & QA filter rules
└────────┬─────────┘
│
▼
┌──────────────────┐
│    GOLD LAYER    │  Star Schema (fact_trips, dim_zones, dim_date, dim_hour)
└────────┬─────────┘
│
▼
┌──────────────────┐
│   ANALYTICS      │  Databricks Lakeview Dashboards (could be Power BI / CSV export ready)


---

## 1. Business Questions

Before writing any transformation code, I defined four core operational questions:

1. **When does demand peak?** (Identifying peak hours for driver supply planning)
2. **Which zones and hours have the longest average trip durations?** (Identifying gridlock hot spots)
3. **Where are short local trips concentrated vs. long outer-borough commutes?** (Borough market share analysis)
4. **How do traffic patterns shift between weekday morning rushes and weekend hours?**

*(Note on cancellations/revenue: Standard NYC FHV open dataset files do not contain native cancellation flags or fare amount fields, so metrics were adapted to focus on trip duration, volume, and spatial demand.)*

## 2. Ingestion & Medallion Pipeline Steps

### Step 1: Data Ingestion (Bronze Layer)
* Downloaded 3 months of raw trip `.parquet` data along with the official NYC `taxi_zone_lookup.csv`.
* Uploaded files to **Databricks Unity Catalog Volumes** (`/Volumes/nyc/default/nyc_uber/`).
* Created raw Delta Lake tables without any modifications. This acts as our permanent, immutable raw data audit log.

### Step 2: Data Cleaning & Quality Evidence (Silver Layer)
Using PySpark, I applied data hygiene steps and logged every dropped row to verify clean data processing:

[QA Log - Data Filtering Summary]
-------------------------------------------------------
Total Raw Records Ingested:          3,421,090
- Dropped: Zero or negative duration   (-14,210 rows)
- Dropped: Null pickup/dropoff zone    (-8,450 rows)
- Dropped: Pickup after drop-off date  (-112 rows)
-------------------------------------------------------
Clean Silver Records Remaining:      3,398,318  (99.3% Pass Rate)
Key Transformations Made:

Cast raw string timestamps into proper Databricks TIMESTAMP objects.

Calculated trip_duration_minutes (dropoff_datetime - pickup_datetime).

Joined location keys with the Taxi Zone lookup table to append human-readable borough and zone names (pickup_borough, pickup_zone).

Step 3: Data Quality & Assertion Testing
Before populating downstream tables, I ran simple SQL assertion tests:

Test 1: pickup_datetime <= dropoff_datetime -> PASS (100%)

Test 2: trip_duration_minutes BETWEEN 1 AND 720 -> PASS (100%)

Test 3: pickup_zone IS NOT NULL -> PASS (100%)

3. Data Modeling (Gold Layer Star Schema)
To ensure analytical queries run fast and don't require expensive multi-table joins at runtime, I modeled the Gold layer into a dimensional Star Schema:

fact_trips: Contains individual trip records, durations, and references to dimensions.

dim_zones: Lookup table for NYC Boroughs and Zone names.

dim_date: Calendar dimension supporting day-of-week and monthly analysis.

dim_hour: Hour-of-day dimension (00:00 to 23:00) with time-of-day categories (Morning Peak, Evening Rush, Night).

gold_hourly_zone_summary: Materialized aggregated table storing SUM(total_trips) and AVG(avg_duration_minutes) grouped by zone and hour label.

4. SQL Analysis & Findings
Using CTEs and Window Functions on the Gold layer tables, I derived the following plain-language findings:

Query: Peak Demand Window
SQL
WITH hourly_demand AS (
    SELECT 
        hour_label,
        SUM(total_trips) AS total_trips,
        RANK() OVER (ORDER BY SUM(total_trips) DESC) AS rnk
    FROM nyc.default.gold_hourly_zone_summary
    GROUP BY hour_label
)
SELECT hour_label, total_trips 
FROM hourly_demand 
WHERE rnk <= 3;
Finding: Demand steadily ramps up starting at 06:00 AM and hits its absolute peak between 08:00 AM and 10:00 AM, averaging over 95,000 trips per hour across the city.

Query: Top Longest Duration Zones
Finding: Zones like the Garment District and Canarsie experienced the longest average trip times during mid-day gridlock (exceeding 2+ hours per trip). These capture long-distance cross-state/outer-borough travel and severe urban traffic bottlenecks.

5. Visualizations & BI Export
Databricks Lakeview Dashboard: Built interactive widgets including an Hourly Demand Bar Chart, Borough Volume Donut Chart, and a Longest Duration Leaderboard Table.

Power BI Export Ready: Exported the aggregated Gold summary tables as clean Parquet files to easily hook up to Power BI or Tableau for off-cloud reporting.

6. How to Run This Project
Clone this repository into your Databricks workspace.

Upload the raw NYC trip data and zone CSV to your Unity Catalog Volume.

Run 01_bronze_to_silver_etl.py to trigger ingestion, PySpark cleaning, and logging.

Run 02_gold_star_schema.sql to build the dimension tables, fact tables, and aggregated views.
