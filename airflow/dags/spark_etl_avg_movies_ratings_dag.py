# airflow/dags/spark_etl_avg_movies_ratings_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="spark_etl_avg_movies_ratings_dag", # DAG name
    start_date=datetime(2025, 8, 1), # Start date
    schedule_interval="@daily", # Run once per day (can be changed)
    catchup=False, # Don't backfill previous runs
    tags=["spark", "etl", "movies"], # Tags for UI organization
) as dag:

    # Task: Run the Spark ETL script using Python
    run_spark_etl = BashOperator(
        task_id="run_spark_etl_pipeline",
        bash_command="python /opt/airflow/dags/../Exercise\ Files/4-spark_etl_avg_movies_ratings_pipeline.py"
    )

    run_spark_etl