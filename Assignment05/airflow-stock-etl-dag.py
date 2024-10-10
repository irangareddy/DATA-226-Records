from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.decorators import task, dag
from datetime import datetime
import requests
from typing import List, Dict, Any

def get_snowflake_connection():
    return SnowflakeHook(snowflake_conn_id='snowflake_connection').get_conn()

@task
def fetch_stock_data(symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch the last 90 days of stock prices for a given symbol
    """
    base_url = Variable.get("alpha_vantage_url")
    api_key = Variable.get("alpha_vantage_api_key")
    
    url = base_url.format(symbol=symbol, alpha_vantage_api_key=api_key)
    
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        stock_data = []
        for date, values in list(data['Time Series (Daily)'].items())[:90]:
            values['date'] = date
            values['symbol'] = symbol
            stock_data.append(values)
        return stock_data
    else:
        error_message = f"Failed to fetch data for symbol {symbol}. Status code: {response.status_code}"
        print(error_message)
        raise ValueError(error_message)

@task
def process_stock_data(raw_stock_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            'symbol': record['symbol'],
            'date': record['date'],
            'open': float(record['1. open']),
            'high': float(record['2. high']),
            'low': float(record['3. low']),
            'close': float(record['4. close']),
            'volume': int(record['5. volume'])
        }
        for record in raw_stock_data
    ]

@task
def upload_to_snowflake(processed_stock_data: List[Dict[str, Any]]):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("USE DATABASE dev")
        cursor.execute("USE SCHEMA dev.raw_data")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            symbol VARCHAR(5),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            PRIMARY KEY (symbol, date)
        )
        """
        cursor.execute("BEGIN")
        cursor.execute(create_table_query)

        merge_query = """
        MERGE INTO stock_data AS target
        USING (
            SELECT
                %s AS symbol,
                %s AS date,
                %s AS open,
                %s AS high,
                %s AS low,
                %s AS close,
                %s AS volume
        ) AS source
        ON target.symbol = source.symbol AND target.date = source.date
        WHEN MATCHED THEN
            UPDATE SET
                target.open = source.open,
                target.high = source.high,
                target.low = source.low,
                target.close = source.close,
                target.volume = source.volume
        WHEN NOT MATCHED THEN
            INSERT (symbol, date, open, high, low, close, volume)
            VALUES (source.symbol, source.date, source.open, source.high, source.low, source.close, source.volume)
        """

        for stock_record in processed_stock_data:
            cursor.execute(merge_query, (
                stock_record['symbol'],
                stock_record['date'],
                stock_record['open'],
                stock_record['high'],
                stock_record['low'],
                stock_record['close'],
                stock_record['volume']
            ))

        cursor.execute("COMMIT")
        print(f"Updated {len(processed_stock_data)} records in stock_data table.")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise Exception(f"An error occurred while uploading data to Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id='stock_price_etl_pipeline',
    description='ETL Pipeline for Daily Stock Prices from AlphaVantage to Snowflake',
    start_date=datetime(2024, 10, 6),
    catchup=False,
    tags=['ETL', 'Stock', 'AlphaVantage', 'Snowflake'],
    schedule='0 16 15 * MON-FRI'
) as dag:
    stock_symbol = Variable.get("stock_symbol", default_var="AAPL")

    raw_stock_data = fetch_stock_data(stock_symbol)
    processed_stock_data = process_stock_data(raw_stock_data)
    upload_to_snowflake(processed_stock_data)
