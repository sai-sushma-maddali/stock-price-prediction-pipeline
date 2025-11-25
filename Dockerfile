# Extend the official Airflow image
FROM apache/airflow:2.10.1

# Switch to airflow user (image already creates /home/airflow)
USER airflow

# Optional: set constraint URL to keep versions compatible with this Airflow release
ARG AIRFLOW_VERSION=2.10.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install dbt + dependencies inside the same venv (no --user, no root)
RUN pip install --no-cache-dir \
    dbt-snowflake \
    yfinance \
    snowflake-connector-python \
    apache-airflow-providers-snowflake \
    --constraint "${CONSTRAINT_URL}"
