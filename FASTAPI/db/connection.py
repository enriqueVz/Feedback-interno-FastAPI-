import snowflake.connector
from fastapi import Depends
#Datos para la conexión con la base de datos
def get_snowflake_connection():
        SNOWFLAKE_USER='SNOWFLAKEROOKIES'
        SNOWFLAKE_PASSWORD='Stemdo01?'
        SNOWFLAKE_ACCOUNT='nagiewe-gk16109'
        SNOWFLAKE_WAREHOUSE='COMPUTE_WH'
        SNOWFLAKE_DATABASE='FEEDBACK'
        SNOWFLAKE_SCHEMA='RAW'
        SNOWFLAKE_ROLE='FEEDBACK'
        
        #Establecer la conexión con la base de datos
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema= SNOWFLAKE_SCHEMA,
            role= SNOWFLAKE_ROLE           
        )
        try:
            yield conn
        finally:
            conn.close()