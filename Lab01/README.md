# Stock Price Prediction: An Integrated Cloud Analytics Solution

## Overview
This project implements an automated stock price prediction system using cloud-based technologies. It leverages Snowflake for data warehousing and machine learning, Apache Airflow for workflow orchestration, and the Alpha Vantage API for real-time stock data.

## Features
- Automated daily stock data collection
- ETL pipeline for data processing
- Machine learning-based stock price forecasting
- Scalable cloud architecture
- Visualization of predictions and historical data

## Technologies Used
- Snowflake
- Apache Airflow
- Alpha Vantage API
- Python

## Setup
1. Clone the repository
2. Set up a Snowflake account and create necessary databases and tables
3. Install Apache Airflow and configure DAGs
4. Obtain an API key from Alpha Vantage
5. Configure Airflow variables with your API key and desired stock symbols

## Usage
1. Start the Airflow webserver and scheduler
2. The data pipeline will run daily, collecting stock data and generating predictions
3. Access forecasts and historical data through Snowflake queries or your preferred visualization tool

## Project Structure
- `stock_data_pipeline.py`: Airflow DAG for data collection and processing
- `stock_model_training_and_prediction.py`: Airflow DAG for model training and prediction

## Contributors
- [Sai Ranga Reddy Nukala](https://github.com/irangareddy)
- [Akshith Reddy Jonnalagadda](https://github.com/akshithreddyj)