## Homework 07

### Overview

In Homework 07, focused on setting up and configuring an Apache Airflow environment using Docker Compose. This includes creating DAGs for data processing and analytics, as well as setting up a Superset instance for data visualization.

### Key Components

1. **Airflow DAGs**:
   - `data_load.py`: This DAG is responsible for loading raw data into Snowflake tables.
   - `analytics_pipeline.py`: This DAG creates analytics tables by joining data from raw tables.

2. **Docker Compose Setup**:
   - `docker-compose.yaml`: Configures the Superset service and its dependencies.
   - `docker-compose-min.yaml`: Provides a minimal setup for an Airflow cluster with CeleryExecutor, Redis, and PostgreSQL.

### Instructions

1. **Set Up Airflow**:
   - Ensure Docker and Docker Compose are installed on your machine.
   - Use `docker-compose-min.yaml` to start the Airflow services:
     ```bash
     docker-compose -f docker-compose-min.yaml up -d
     ```

2. **Set Up Superset**:
   - Use `docker-compose.yaml` to start the Superset service:
     ```bash
     docker-compose -f docker-compose.yaml up -d
     ```

3. **Access Services**:
   - Airflow Web UI: [http://localhost:8081](http://localhost:8081)
   - Superset Web UI: [http://localhost:8088](http://localhost:8088)

### Notes

- Ensure that the environment variables in the Docker Compose files are correctly set up for your local environment.
- Follow the instructions in the comments of `docker-compose-min.yaml` for setting up the Airflow user and other configurations.
