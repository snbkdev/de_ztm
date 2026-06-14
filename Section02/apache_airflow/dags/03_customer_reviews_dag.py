from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
import os, csv

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

@dag(
    "customer_reviews_dag",
    start_date=datetime(2026, 6, 10),
    schedule = "* * * * *",
    catchup=False,
    description="Review average score",
)

def customer_reviews_dag():

    @task
    def extract_reviews():
        pg_hook = PostgresHook(postgres_conn_id = "postgres_bookings")

        context = get_current_context()
        execution_date = context["data_interval_start"]
        start_of_minute = execution_date.replace(second=0, microsecond=0)
        end_of_minute = start_of_minute + timedelta(minutes=1)

        start_of_minute_plus_6 = start_of_minute + timedelta(hours=5)
        end_of_minute_plus_6 = end_of_minute + timedelta(hours=7)

        query = f"""
            select review_id, listing_id, review_score, review_comment, review_date from customer_reviews
            where review_date >= '{start_of_minute_plus_6.strftime('%Y-%m-%d %H:%M:%S')}'
            and review_date < '{end_of_minute_plus_6.strftime('%Y-%m-%d %H:%M:%S')}'
        """

        records = pg_hook.get_records(query)
        column_names = ["review_id", "listing_id", "review_score", "review_comment", "review_date"]

        #file_date = execution_date.strftime('%Y%m%d_%H%M')
        file_path = ""

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(column_names)
            writer.writerows(records)

        print(f"Customer reviews written to {file_path}")

    spark_etl = SparkSubmitOperator(
        task_id = "spark_etl_reviews",
        application=os.path.join(DAG_FOLDER, 'spark_etl_reviews.py'),
        name="guest_reviews_etl",
        application_args=[
            "--customer_reviews", "",
            "--output_path", ""
        ],
        conn_id='spark_booking',
        conf={
        "spark.master": "local[*]"  # Локальный режим
        }
    )

    extract_task = extract_reviews()
    extract_task >> spark_etl

dag_instance = customer_reviews_dag()