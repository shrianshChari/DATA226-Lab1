from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pandas as pd
import yfinance as yf
import snowflake.connector

stock_symbols = ['AAPL', 'NVDA']  # Add more symbols as needed

def return_snowflake_cursor() -> snowflake.connector.cursor.SnowflakeCursor:
    conn = snowflake.connector.connect(
        user=Variable.get('snowflake_user'),
        password=Variable.get('snowflake_password'),
        account=Variable.get('snowflake_account'),
        warehouse='compute_wh'
    )
    return conn.cursor()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'yfinance_to_snowflake_to_forecasting',
    default_args=default_args,
    description='Fetch stock prices and load into Snowflake, then train forecasting model and generate predictions',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2025, 3, 1),  # Update the start date as needed
    catchup=False
)
def lab1_dag():
    @task
    def extract(stock_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch stock data from yfinance."""
        stock = yf.download(stock_symbol, start=start_date, end=end_date)
        logging.debug(f'Extracting data for symbol {stock_symbol}.')
        if stock is not None:
            return stock
        else:
            raise ValueError('Stock information is None.')

    @task
    def transform(raw_data: pd.DataFrame) -> pd.DataFrame:
        stock_symbol = raw_data.columns.get_level_values(1).unique()[-1]

        logging.debug(f'Transforming data for symbol {stock_symbol}.')

        raw_data.reset_index(inplace=True)
        raw_data["Symbol"] = stock_symbol
        raw_data.columns = raw_data.columns.droplevel(1)
        return raw_data

    @task
    def combine_transformed_data(transformed_dfs: list[pd.DataFrame]) -> pd.DataFrame:
        logging.debug(f'Combining dataframes together.')
        return pd.concat(transformed_dfs, ignore_index=True)

    @task
    def load(cursor: snowflake.connector.cursor.SnowflakeCursor, stock_data: pd.DataFrame, table_name: str,
                          database: str = 'dev', schema: str = 'stock_schema'):
        """Load data into Snowflake."""
        try:
            cursor.execute("BEGIN;")

            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};")

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table_name} (
                stock_symbol STRING,
                date DATE,
                open FLOAT,
                close FLOAT,
                min FLOAT,
                max FLOAT,
                volume BIGINT
            );"""
            logging.debug(create_table_query)
            cursor.execute(create_table_query)

            cursor.execute(f"DELETE FROM {database}.{schema}.{table_name};")

            # Insert data
            for _, row in stock_data.iterrows():
                insert_query = f"""
                INSERT INTO {database}.{schema}.{table_name} (stock_symbol, date, open, close, min, max, volume)
                VALUES ('{row["Symbol"]}', TO_DATE('{row["Date"]}'), {row["Open"]},
                {row["Close"]}, {row["Low"]}, {row["High"]}, {row["Volume"]});
                """
                logging.debug(insert_query)
                cursor.execute(insert_query)

            cursor.execute("COMMIT;")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            print(e)
            raise e

    @task
    def train(cursor: snowflake.connector.cursor.SnowflakeCursor, table_name: str, model_name: str,
              database: str = 'dev', schema: str = 'stock_schema'):
        try:
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")

            # Creating forecast model
            create_model_sql = f'''
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {model_name} (
                INPUT_DATA => TABLE(SELECT date, stock_symbol, close FROM {database}.{schema}.{table_name}),
                SERIES_COLNAME => 'stock_symbol',
                TIMESTAMP_COLNAME => 'date',
                TARGET_COLNAME => 'close',
                CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
            );
            '''
            logging.debug(create_model_sql)
            cursor.execute(create_model_sql)

            cursor.execute(f'CALL {model_name}!SHOW_EVALUATION_METRICS();')
        except Exception as e:
            print(e)
            raise

    @task
    def predict(cursor: snowflake.connector.cursor.SnowflakeCursor, training_table_name: str, model_name: str,
                forecast_table_name: str, final_table_name: str,
                database: str = 'dev', schema: str = 'stock_schema'):
        try:
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")


            # Creation of table
            create_forecast_table_sql = f'''
            BEGIN
            CALL {model_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );

            CREATE OR REPLACE TABLE {forecast_table_name} AS
            SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
            END;
            '''

            logging.debug(create_forecast_table_sql)
            cursor.execute(create_forecast_table_sql)

            # Creation of unioned table
            create_final_table_sql = f'''
            CREATE OR REPLACE TABLE {final_table_name} AS
            SELECT STOCK_SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {training_table_name}
            UNION ALL
            SELECT replace(series, '"', '') as STOCK_SYMBOL, TO_DATE(ts) as DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {forecast_table_name};
            '''
            logging.debug(create_final_table_sql)
            cursor.execute(create_final_table_sql)
        except Exception as e:
            print(e)
            raise


    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    database = 'dev'
    schema = 'stock_schema'
    table_name = 'STOCK_PRICES'

    model_name = 'm'

    forecast_table_name = 'forecasted'
    final_table_name = 'final'

    cursor = return_snowflake_cursor()

    transformed_dfs = []
    for symbol in stock_symbols:
        raw_data = extract(symbol, start_date, end_date)
        stock_data = transform(raw_data)
        transformed_dfs.append(stock_data)
    
    total_data = combine_transformed_data(transformed_dfs)

    load_task_instance = load(cursor, total_data, table_name, database, schema)

    train_task_instance = train(cursor, table_name, model_name)

    predict_task_instance = predict(cursor, table_name, model_name, forecast_table_name, final_table_name)

    load_task_instance >> train_task_instance >> predict_task_instance

lab1_dag()
