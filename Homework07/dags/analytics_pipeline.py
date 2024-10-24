from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

analytics_dag = DAG(
    'snowflake_analytics_processing',
    default_args=default_args,
    description='Create analytics tables with joined data',
    schedule_interval='@daily',
    catchup=False
)

create_analytics_schema = SnowflakeOperator(
    task_id='create_analytics_schema',
    sql="CREATE SCHEMA IF NOT EXISTS dev.analytics;",
    snowflake_conn_id='snowflake_conn',
    dag=analytics_dag
)

create_session_summary = SnowflakeOperator(
    task_id='create_session_summary',
    sql="""
    CREATE OR REPLACE TABLE dev.analytics.session_summary AS
    WITH deduplicated_sessions AS (
        SELECT 
            sessionId,
            ts,
            ROW_NUMBER() OVER (PARTITION BY sessionId ORDER BY ts) as rn
        FROM dev.raw_data.session_timestamp
        WHERE ts IS NOT NULL
    )
    SELECT 
        usc.userId,
        usc.sessionId,
        usc.channel,
        st.ts as session_timestamp,
        DATE_TRUNC('WEEK', st.ts) as session_week
    FROM dev.raw_data.user_session_channel usc
    JOIN deduplicated_sessions st ON usc.sessionId = st.sessionId
    WHERE st.rn = 1;  -- Only keep the first occurrence of each sessionId
    """,
    snowflake_conn_id='snowflake_conn',
    dag=analytics_dag
)

create_analytics_schema >> create_session_summary