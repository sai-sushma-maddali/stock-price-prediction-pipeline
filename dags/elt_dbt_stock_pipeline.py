from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dbt_elt_pipeline",
    start_date=datetime(2025, 11, 23),
    schedule_interval="45 0 * * *",
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/stock_analytics && dbt run --profiles-dir ."
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/stock_analytics && dbt test --profiles-dir ."
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt/stock_analytics && dbt snapshot --profiles-dir ."
    )

    dbt_run >> dbt_test >> dbt_snapshot