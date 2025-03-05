from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from pandas import DataFrame
import yfinance as yf
import snowflake.connector

def fetch_stock_data(stock_symbol, start_date, end_date):
    """Fetch stock data from yfinance."""
    stock = yf.download(stock_symbol, start=start_date, end=end_date)
    stock.reset_index(inplace=True)
    return stock

def load_to_snowflake(symbol: str, stock_data: DataFrame, table_name: str,
                      database: str = 'dev', schema: str = 'stock_schema'):
    """Load data into Snowflake."""
    conn = snowflake.connector.connect(
        user=Variable.get('snowflake_user'),
        password=Variable.get('snowflake_password'),
        account=Variable.get('snowflake_account')
    )
    cursor = conn.cursor()

    if (not table_name.endswith(symbol)):
        table_name = table_name + "_" + symbol.upper()

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
        cursor.execute(create_table_query)

        cursor.execute(f"DELETE FROM {database}.{schema}.{table_name};")

        # Insert data
        for _, row in stock_data.iterrows():
            insert_query = f"""
            INSERT INTO {database}.{schema}.{table_name} (stock_symbol, date, open, close, min, max, volume)
            VALUES ('{symbol}', TO_DATE('{row["Date"].item()}'), {row["Open"].item()},
            {row["Close"].item()}, {row["Low"].item()}, {row["High"].item()}, {row["Volume"].item()});
            """
            # logging.info(insert_query)
            cursor.execute(insert_query)

        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e
    finally:
        cursor.close()
        conn.close()

def etl_task(**kwargs):
    """Main ETL task to fetch stock data and load it into Snowflake."""
    stock_symbols = ['AAPL', 'NVDA']  # Add more symbols as needed
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    database = 'dev'
    schema = 'stock_schema'
    table_name = 'STOCK_PRICES'

    for symbol in stock_symbols:
        stock_data = fetch_stock_data(symbol, start_date, end_date)
        if stock_data is not None:
            load_to_snowflake(symbol, stock_data, table_name, database, schema)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'yfinance_to_snowflake',
    default_args=default_args,
    description='Fetch stock prices and load into Snowflake',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2025, 3, 1),  # Update the start date as needed
    catchup=False,
)

# Define the PythonOperator
etl_task = PythonOperator(
    task_id='fetch_and_load_stock_data',
    python_callable=etl_task,
    dag=dag
)

etl_task
