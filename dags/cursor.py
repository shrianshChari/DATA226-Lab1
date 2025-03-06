from airflow.models import Variable

import snowflake.connector

def return_snowflake_cursor() -> snowflake.connector.cursor.SnowflakeCursor:
    conn = snowflake.connector.connect(
        user=Variable.get('snowflake_user'),
        password=Variable.get('snowflake_password'),
        account=Variable.get('snowflake_account'),
        warehouse='compute_wh'
    )
    return conn.cursor()
