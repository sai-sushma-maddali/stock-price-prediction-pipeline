from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests

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
def train(cursor, train_input_table, train_view, forecast_function_name):
    '''Trains a forecasting model using Snowflake's ML capabilities.'''

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS
    SELECT TO_TIMESTAMP_NTZ(DATE) AS DATE_v1, CLOSE, SYMBOL FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name}(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE_v1',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
);
"""

    try:
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        # Inspect the accuracy metrics of your model. 
        cursor.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")

        print("Model training completed successfully.")
    except Exception as e:
        print("Exception while training the model: ",e)
        raise

@task
def predict(cursor, forecast_function_name, train_input_table, forecast_table, final_table):
    '''Generates forecasts using the trained model and stores results in a final table.'''

    # Forecasting for the next 7 days
    make_prediction_sql = f"""CALL {forecast_function_name}!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => {{'prediction_interval': 0.95}}
);"""
    
    # Store the forecast results in a table
    store_forecast_sql = f""" CREATE OR REPLACE TABLE {forecast_table} AS 
    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  
    """
    # Create a final table combining actuals and forecasts
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(store_forecast_sql)
        cursor.execute(create_final_table_sql)
        print("Forecasting and storing results completed successfully.")
    except Exception as e:
        print("Exception while forecasting stock prices: ", e)
        raise


with DAG(
    dag_id = 'daily_stock_price_forecasting',
    start_date = datetime(2025,10,4),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule_interval = "30 0 * * *"  # Daily at 12:30 AM UTC
) as dag:

    # train_input_table = "raw.yfinance_stock"
    # train_view = "adhoc.stock_data_view"
    # forecast_table = "adhoc.stock_data_forecast"
    # forecast_function_name = "analytics.predict_stock_price"
    # final_table = "analytics.stock_actuals_forecast"

    train_input_table = Variable.get("stock_table_name")
    train_view = Variable.get("train_view_name")
    forecast_table = Variable.get("forecast_table_name")
    forecast_function_name = Variable.get("forecast_function_name")
    final_table = Variable.get("final_table_name")



    cur = return_snowflake_conn()

    train_task = train(cur, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)
    train_task >> predict_task