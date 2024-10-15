from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
from airflow.decorators import task
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

@task
def extract(symbols, api_key):
    all_results = {}
    try:
        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
            res = requests.get(url).json()
            results = []
            for d in res["Time Series (Daily)"]:
                daily_info = res["Time Series (Daily)"][d]
                daily_info['6. date'] = d
                daily_info['7. symbol'] = symbol
                results.append(daily_info)
            all_results[symbol] = results[-90:]  # Last 90 days
    except Exception as e:
        print(f"Error extracting data for {symbol}: {e}")
    return all_results

@task
def transform(data):
    entries = []
    try:
        for symbol, entry_list in data.items():
            entries.extend(entry_list)
    except Exception as e:
        print(f"Error transforming data: {e}")
    return entries

@task
def load(con, results, target_table):
    try:
        con.execute("BEGIN")
        con.execute(f"""CREATE OR REPLACE TABLE {target_table} (
                symbol VARCHAR(10) NOT NULL,
                date TIMESTAMP_NTZ NOT NULL,
                open DECIMAL(10, 4) NOT NULL,
                high DECIMAL(10, 4) NOT NULL,
                low DECIMAL(10, 4) NOT NULL,
                close DECIMAL(10, 4) NOT NULL,
                volume BIGINT NOT NULL,
                PRIMARY KEY (symbol,date)
            )""")
        for r in results:
            open = r['1. open']
            high = r['2. high']
            low = r['3. low']
            close = r['4. close']
            volume = r['5. volume']
            date = r['6. date']
            symbol = r['7. symbol']
            con.execute(f"""INSERT INTO {target_table} (symbol, date, open, high, low, close, volume) 
                            VALUES ('{symbol}', '{date}', {open}, {high}, {low}, {close}, {volume})""")
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"Error loading data: {e}")

with DAG(
    dag_id='stock_data_pipeline',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    target_table = "dev.raw_data.alphavantage_stockprice"
    api_key = Variable.get("alpha_vantage_api")
    symbols = Variable.get("stock_symbols", default_var=['NVDA', 'ORCL'])

    # Task flow
    extract_task = extract(symbols, api_key)
    transform_task = transform(extract_task)
    load_task = load(return_snowflake_conn(), transform_task, target_table)

    extract_task >> transform_task >> load_task

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_model_training_pipeline',
        trigger_dag_id='stock_model_training_and_prediction',
        wait_for_completion=False
    )

    load_task >> trigger_next_dag