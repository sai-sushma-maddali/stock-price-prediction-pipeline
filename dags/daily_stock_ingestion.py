from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.decorators import dag
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import yfinance as yf
import pandas as pd

def return_snowflake_conn():
    '''Establishes and returns a Snowflake connection using Airflow's SnowflakeHook.'''
    try:
        # Initialize SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        
        # Execute the query to fetch results
        conn = hook.get_conn()
        return conn.cursor()
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

@task
def extract_stock_prices():
    '''Fetches stock prices for the last 180 days for specified ticker symbols using yfinance.'''
    try:
        # ticker_symbols = ["AMZN","GOOG"]
        ticker_symbols = Variable.get("stock_ticker_list", default_var="AMZN,GOOG").split(",")
        stock_data = yf.download(tickers = ticker_symbols, group_by = "ticker", period="180d")

        return stock_data
    except Exception as e:
        print("Error fetching stock prices from yfinance",e)
        return pd.DataFrame()


@task
def transform_stock_data(stock_data):
  '''Transforms the raw stock data into a structured format suitable for loading into Snowflake.'''
  
  try:
   
    stacked_data = stock_data.stack(level=0, future_stack=True).reset_index()
    stacked_data.columns.name = None
    # Preparing data in required structure
    stacked_data = stacked_data[['Ticker', 'Date', 'Open', 'Close', 'Low', 'High', 'Volume']]
    stacked_data = stacked_data.rename(columns = {'Ticker':'StockSymbol', 'Low':'Min', 'High': 'Max'})
    return stacked_data
  except Exception as e:
    print("Error fetching stock prices from yfinance",e)
    return pd.DataFrame()

# helper function to insert data into Snowflake
def insert_records(data, cursor, stock_table):
    '''Inserts records into the Snowflake table.'''
    try:

        for idx, row in data.iterrows():
            symbol = row.StockSymbol
            date = row.Date
            open = row.Open
            close = row.Close
            min = row.Min
            max = row.Max
            volume = row.Volume

            insert_sql = f"""INSERT INTO {stock_table} (Symbol, Date, Open, Close, Min, Max, Volume) VALUES ('{symbol}','{date}', {open}, {close}, {min}, {max}, {volume})"""

            cursor.execute(insert_sql)
    except Exception as e:
        print("Error inserting records into Snowflake", e)
        raise

@task 
def load_stock_prices_snowflake(data, stock_table):
    '''Loads the transformed stock data into a Snowflake table.'''
    cursor = return_snowflake_conn()
    try:
        # Creating table
        create_table_sql = f"""

CREATE TABLE IF NOT EXISTS {stock_table}(
  Symbol VARCHAR(5) NOT NULL,
  Date DATE NOT NULL,
  Open FLOAT NOT NULL,
  Close FLOAT NOT NULL,
  Min FLOAT NOT NULL,
  Max FLOAT NOT NULL,
  Volume FLOAT NOT NULL,
  PRIMARY KEY(Symbol, Date)

)

"""
        cursor.execute("BEGIN;")
        cursor.execute(create_table_sql)

        # clear table
        cursor.execute("DELETE FROM raw.yfinance_stock")

        # Insert into table
        insert_records(data, cursor, stock_table)
        cursor.execute("COMMIT;")
        return "Successfully loaded data into Snowflake"
    except Exception as e:
        print("Exception in ETL pipeline: ", e)
        cursor.execute("ROLLBACK;")
        raise
   

trigger_forecast = TriggerDagRunOperator(
    task_id="trigger_forecast_dag",
    trigger_dag_id="daily_stock_price_forecasting",  # ID of forecast DAG
    wait_for_completion=True,  # Optional: wait until forecast DAG finishes
    reset_dag_run=True,        # Optional: ensures fresh run
    poke_interval=60,          # Optional: polling interval
)

# Defining the DAG

@dag(
    dag_id="daily_yfinance_stock_ingestion",
    start_date=datetime(2025, 10, 4),
    catchup=False,
    schedule_interval="0 0 * * *",
    tags=["ETL"]
)
def etl_pipeline():
    stock_table = Variable.get("stock_table_name")

    stock_data = extract_stock_prices()
    transformed_data = transform_stock_data(stock_data)
    loaded = load_stock_prices_snowflake(transformed_data, stock_table)

    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_dag",
        trigger_dag_id="daily_stock_price_forecasting",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60,
    )

    loaded >> trigger_forecast

etl_pipeline()
