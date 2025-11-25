
## Stock Price Analytics & Forecasting Pipeline

### End-to-End System Using **Snowflake, Airflow, dbt, and Power BI**

This project implements a complete production-style data analytics pipeline that extracts stock price data, loads it into Snowflake, generates ML-based forecasts, transforms data using dbt, and visualizes insights in Power BI.

---
##  Project Overview

This pipeline performs the following steps every day:

### **ETL (Airflow)**

* Extract stock prices using *yfinance*
* Transform into a structured format
* Load into Snowflake RAW tables
* Wraps loads in SQL transactions (BEGIN / COMMIT / ROLLBACK)
* Triggers downstream pipelines programmatically

### **Forecasting (Snowflake ML + Airflow)**

* Uses `SNOWFLAKE.ML.FORECAST`
* Trains a daily forecasting model
* Generates 7-day predictions
* Merges forecast output with historical actuals

### **ELT (dbt + Airflow)**

* Cleans and stages data (`stg_prices`)
* Computes analytical indicators:

  * Daily Return
  * 7-Day Moving Average
  * 30-Day Moving Average
  * 30-Day Volatility
  * RSI (14-Day)
* Applies dbt tests (unique + not null)
* Snapshotting to capture historical model drift
* dbt is scheduled as its own Airflow DAG and triggered automatically after forecast completes

### **BI Dashboard (Power BI)**

* Price trends
* Actual vs Forecast
* Moving averages
* RSI scatter
* Trading volumes
* Forecast range visualization

---

###  DAG Overview

#### 1. daily_yfinance_stock_ingestion
- **Purpose**: Extracts historical stock prices using yfinance, transforms the data, and loads it into a Snowflake table.
- **Schedule**: Daily at midnight UTC (0 0 * * *)
- **Steps**:
  - extract_stock_prices: Downloads 180 days of data for configured tickers.
  - transform_stock_data: Reshapes and renames columns for Snowflake compatibility.
  - load_stock_prices_snowflake: Creates and populates the target table.
  - TriggerDagRunOperator: Triggers the forecast DAG upon successful load.

#### 2. daily_stock_price_forecasting
- **Purpose**: Trains a time series forecasting model and stores predictions.
- **Schedule**: Daily at 12:30 AM UTC (30 0 * * *)
- **Steps**:
  - train: Creates a view and trains a model using SNOWFLAKE.ML.FORECAST.
  - predict: Generates 7-day forecasts and merges them with actuals.

#### 3. dbt_elt_pipeline

* **Purpose**: Executes all ELT transformations using dbt to build analytics-ready tables in Snowflake.
  This includes calculating technical indicators such as moving averages (7-day, 30-day), daily returns, RSI(14), and 30-day volatility.
  It also performs automated data quality testing and snapshotting for historical tracking.
* **Schedule**: Daily at **12:45 AM UTC** (`45 0 * * *`) and also triggered automatically after the forecasting DAG completes.
* **Steps**:

  * **dbt run**

    * Executes the dbt project
    * Builds staging and mart models including `stg_prices` and `fact_indicators`
    * Materializes the analytics tables in the `ANALYTICS` schema
  * **dbt test**

    * Runs data quality tests (unique, not null, referential checks)
    * Ensures the accuracy and reliability of Snowflake data
  * **dbt snapshot**

    * Captures historical versions of selected tables
    * Enables model-drift monitoring and auditability over time


---
## dbt Project (stock_analytics)

### **Staging Model (`stg_prices`)**

* Standardizes symbol formatting
* Corrects datatypes
* Removes duplicates
* Materialized as a **view** in `ANALYTICS`

### **Mart Model (`fact_indicators`)**

Computes:

* 7-day Moving Average
* 30-day Moving Average
* Daily Return
* 30-day Rolling Volatility
* RSI (14-day)

Materialized as a **table** in `ANALYTICS`.

### **dbt Tests**

* `unique` on (`symbol`, `trade_date`)
* `not_null` tests on important fields

### **dbt Snapshot**

Captures historical changes in forecasted values (optional but included).

---
## Power BI Dashboard

A Power BI dashboard has been built on top of Snowflake’s ANALYTICS tables.
It includes:

* **Latest close price**
* **% change from previous day**
* **30-day average volume**
* **Closing price trend**
* **Actual vs Forecast**
* **Moving averages (7-day, 30-day)**
* **RSI vs Daily Return scatter plot**
* **As-of position within forecast range**
* **Monthly trading volume**

---

## Snowflake Tables Used

| Layer     | Table                              | Purpose                              |
| --------- | ---------------------------------- | ------------------------------------ |
| RAW       | `RAW.yfinance_stock`               | Cleaned historical stock prices      |
| ADHOC     | `adhoc.stock_data_forecast`        | Forecast model output                |
| ANALYTICS | `analytics.stock_actuals_forecast` | Merged actual/forecast data          |
| ANALYTICS | `analytics.fact_indicators`        | Computed technical indicators for BI |

---
### Airflow Variables

| Variable Name             | Purpose                                      | Example Value                  |
|--------------------------|----------------------------------------------|--------------------------------|
| stock_ticker_list      | Comma-separated list of tickers              | AMZN,GOOG                    |
| stock_table_name       | Target table for raw stock prices            | RAW.yfinance_stock          |
| train_view_name        | View used for model training                 | adhoc.stock_data_view       |
| forecast_table_name    | Table to store forecast results              | adhoc.stock_data_forecast   |
| forecast_function_name | Name of the Snowflake ML function            | analytics.predict_stock_price |
| final_table_name       | Merged table of actuals and forecasts        | analytics.stock_actuals_forecast |

Set these in the Airflow UI under **Admin → Variables** or via CLI:
bash
airflow variables set stock_ticker_list "AMZN,GOOG"


---

### Dependencies

- Airflow 2.x
- Snowflake Python Connector
- Airflow Snowflake Provider
- yfinance
- pandas

---

### Setup Notes

* Ensure your Snowflake connection is configured in Airflow as **`snowflake_conn`** under *Admin → Connections*.
* The **ETL DAG** (`daily_yfinance_stock_ingestion`) triggers both the **forecasting DAG** and the **dbt ELT DAG** using `TriggerDagRunOperator`.
* All Snowflake write operations in the ETL pipeline are wrapped inside **SQL transactions** (`BEGIN`, `COMMIT`, `ROLLBACK`) to ensure idempotency and data reliability.
* Forecasting is performed using **Snowflake’s native ML** via `SNOWFLAKE.ML.FORECAST`.
* The **dbt ELT pipeline** (`dbt_elt_pipeline`) is scheduled independently and also triggered automatically after the forecasting DAG completes.
* dbt commands (`dbt run`, `dbt test`, `dbt snapshot`) are executed inside Airflow using a custom Airflow image with `dbt-snowflake` installed.
* The dbt project is mounted inside the Airflow container at `/opt/airflow/dbt/stock_analytics`, and Airflow runs dbt using `--profiles-dir .`.
* Power BI (or Superset/Presets) connects directly to Snowflake’s **ANALYTICS** schema to visualize transformed tables such as `fact_indicators` and `stock_actuals_forecast`.

---
