from datetime import datetime, timedelta
import logging

from airflow.decorators import task, dag

import snowflake.connector

from cursor import return_snowflake_cursor

CONFIG = {
    "database": "dev",
    "schema": "stock_schema",
    "table_name": "STOCK_PRICES",
    "model_name": "m",
    "forecast_table_name": "forecasted",
    "final_table_name": "final"
}

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
    'train_predict_model',
    default_args=default_args,
    description='Take data stored in Snowflake, train forecasting model and generate predictions'
)
def forecasting_dag():
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

    database = CONFIG['database']
    schema = CONFIG['schema']
    table_name = CONFIG['table_name']

    model_name = CONFIG['model_name']

    forecast_table_name = CONFIG['forecast_table_name']
    final_table_name = CONFIG['final_table_name']

    cursor = return_snowflake_cursor()

    train_task_instance = train(cursor, table_name, model_name, database, schema)

    predict_task_instance = predict(cursor, table_name, model_name, forecast_table_name,
                                    final_table_name, database, schema)

    train_task_instance >> predict_task_instance

forecasting_dag()
