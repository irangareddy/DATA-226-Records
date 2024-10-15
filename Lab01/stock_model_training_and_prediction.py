from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.decorators import task
from airflow.models import Variable

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()

@task
def train_model(cur, train_input_table, train_view, forecast_function_name):
    # SQL for creating the view
    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    # SQL for creating the forecast model
    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        # Create the view for training
        print(f"Creating view: {train_view}")
        cur.execute(create_view_sql)
        print(f"View {train_view} created successfully")

        # Create the forecast model
        print(f"Creating forecast model: {forecast_function_name}")
        cur.execute(create_model_sql)
        print(f"Forecast model {forecast_function_name} created successfully")

        # Call the function to show evaluation metrics
        print(f"Calling SHOW_EVALUATION_METRICS for {forecast_function_name}")
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        print(f"Evaluation metrics for {forecast_function_name} fetched successfully")

    except Exception as e:
        print(f"Error during model training: {str(e)}")
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    # SQL to generate predictions
    make_prediction_sql = f"""BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    # SQL for creating the final table that merges actual and forecast data
    create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT REPLACE(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};"""

    try:
        # Make the forecast and create the forecast table
        print(f"Generating predictions using {forecast_function_name}")
        cur.execute(make_prediction_sql)
        print(f"Prediction table {forecast_table} created successfully")

        # Create the final table with actual and forecast data
        print(f"Creating final result table {final_table}")
        cur.execute(create_final_table_sql)
        print(f"Final result table {final_table} created successfully")

    except Exception as e:
        print(f"Error during prediction: {str(e)}")
        raise

# Define the DAG for model training and prediction
with DAG(
    dag_id='stock_model_training_and_prediction',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    train_input_table = "dev.raw_data.alphavantage_stockprice"
    train_view = "dev.adhoc.training_view"
    forecast_table = "dev.adhoc.stockprice_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.stockprice_finalprediction"

    train_task = train_model(return_snowflake_conn(), train_input_table, train_view, forecast_function_name)
    predict_task = predict(return_snowflake_conn(), forecast_function_name, train_input_table, forecast_table, final_table)

    train_task >> predict_task
