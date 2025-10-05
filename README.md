
## Stock Price Forecasting Pipeline with Airflow & Snowflake

This repository contains two Airflow DAGs that orchestrate a daily pipeline for extracting stock prices, loading them into Snowflake, training a forecasting model, and generating 7-day predictions using Snowflake's native ML capabilities.


###  DAG Overview

#### 1. `daily_yfinance_stock_ingestion`
- **Purpose**: Extracts historical stock prices using `yfinance`, transforms the data, and loads it into a Snowflake table.
- **Schedule**: Daily at midnight UTC (`0 0 * * *`)
- **Steps**:
  - `extract_stock_prices`: Downloads 180 days of data for configured tickers.
  - `transform_stock_data`: Reshapes and renames columns for Snowflake compatibility.
  - `load_stock_prices_snowflake`: Creates and populates the target table.
  - `TriggerDagRunOperator`: Triggers the forecast DAG upon successful load.

#### 2. `daily_stock_price_forecasting`
- **Purpose**: Trains a time series forecasting model and stores predictions.
- **Schedule**: Daily at 12:30 AM UTC (`30 0 * * *`)
- **Steps**:
  - `train`: Creates a view and trains a model using `SNOWFLAKE.ML.FORECAST`.
  - `predict`: Generates 7-day forecasts and merges them with actuals.

---

### Airflow Variables

| Variable Name             | Purpose                                      | Example Value                  |
|--------------------------|----------------------------------------------|--------------------------------|
| `stock_ticker_list`      | Comma-separated list of tickers              | `AMZN,GOOG`                    |
| `stock_table_name`       | Target table for raw stock prices            | `RAW.yfinance_stock`          |
| `train_view_name`        | View used for model training                 | `adhoc.stock_data_view`       |
| `forecast_table_name`    | Table to store forecast results              | `adhoc.stock_data_forecast`   |
| `forecast_function_name` | Name of the Snowflake ML function            | `analytics.predict_stock_price` |
| `final_table_name`       | Merged table of actuals and forecasts        | `analytics.stock_actuals_forecast` |

Set these in the Airflow UI under **Admin → Variables** or via CLI:
```bash
airflow variables set stock_ticker_list "AMZN,GOOG"
```

---

### Dependencies

- Airflow 2.x
- Snowflake Python Connector
- Airflow Snowflake Provider
- yfinance
- pandas

---

###  Setup Notes

- Ensure your Snowflake connection is configured in Airflow as `snowflake_conn`.
- The ETL DAG triggers the forecast DAG using `TriggerDagRunOperator`.
- All SQL operations are wrapped in transactions for reliability.
- Forecasting uses Snowflake’s native ML with `SNOWFLAKE.ML.FORECAST`.

---

###  Output Tables

- `RAW.yfinance_stock`: Raw ingested prices
- `adhoc.stock_data_forecast`: Forecasted values
- `analytics.stock_actuals_forecast`: Final merged table for reporting