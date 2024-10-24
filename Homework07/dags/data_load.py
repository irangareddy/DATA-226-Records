from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

raw_data_dag = DAG(
    'snowflake_raw_data_load',
    default_args=default_args,
    description='Load raw data into Snowflake tables',
    schedule_interval='@daily',
    catchup=False
)

create_user_session_channel = SnowflakeOperator(
    task_id='create_user_session_channel',
    sql="""
    CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
        userId int not NULL,
        sessionId varchar(32) primary key,
        channel varchar(32) default 'direct'  
    );
    """,
    snowflake_conn_id='snowflake_conn',
    dag=raw_data_dag
)

create_session_timestamp = SnowflakeOperator(
    task_id='create_session_timestamp',
    sql="""
    CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
        sessionId varchar(32) primary key,
        ts timestamp  
    );
    """,
    snowflake_conn_id='snowflake_conn',
    dag=raw_data_dag
)

create_stage = SnowflakeOperator(
    task_id='create_stage',
    sql="""
    CREATE OR REPLACE STAGE dev.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """,
    snowflake_conn_id='snowflake_conn',
    dag=raw_data_dag
)

load_user_session_channel = SnowflakeOperator(
    task_id='load_user_session_channel',
    sql="""
    COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=raw_data_dag
)

load_session_timestamp = SnowflakeOperator(
    task_id='load_session_timestamp',
    sql="""
    COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv;
    """,
    snowflake_conn_id='snowflake_conn',
    dag=raw_data_dag
)

[create_user_session_channel, create_session_timestamp] >> create_stage
create_stage >> [load_user_session_channel, load_session_timestamp]
