from airflow import DAG, task
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import get_current_context
import random
import os, csv
from datetime import datetime, timedelta

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))


def read_bookings_from_postgres():
    context = get_current_context()
    execution_date = context["data_interval_start"]
    
    file_date = execution_date.strftime("%Y-%m-%d_%H%M")
    file_path = f""

    start_of_minute = execution_date.replace(second=0, microsecond=0)
    end_of_minute = start_of_minute + timedelta(minutes=1)

    start_of_minute_plus_6 = start_of_minute + timedelta(hours=6)
    end_of_minute_plus_6 = end_of_minute + timedelta(hours=6)

    pg_hook = PostgresHook(postgres_conn_id="postgres_bookings")
    query = f"""
        select booking_id, listing_id, user_id, booking_time, status from bookings
        where booking_time >= '{start_of_minute_plus_6.strftime('%Y-%m-%d %H:%M:%S')}'
        and booking_time < '{end_of_minute_plus_6.strftime('%Y-%m-%d %H:%M:%S')}'
        """

    records = pg_hook.get_records(query)

    bookings = []

    print(f"Read {len(records)} from Postgres")
    for record in records:
        booking = {
            "booking_id": record[0],
            "listing_id": record[1],
            "user_id": record[2],
            "booking_time": record[3].strftime('%Y-%m-%d %H:%M:%S'),
            "status": record[4]
        }
        bookings.append(booking)

    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    fieldnames = ["booking_id", "listing_id", "user_id", "booking_time", "status"]

    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for booking in bookings:
            writer.writerow({
                "booking_id": booking["booking_id"],
                "listing_id": booking["listing_id"],
                "user_id": booking["user_id"],
                "booking_time": booking["booking_time"],
                "status": booking["status"],
            })

    print(f"Generated bookings data written to {file_path}")

    return file_path

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 11),
    'catchup': False,
}

with DAG(
    dag_id='bookings_spark_pipeline',
    default_args=default_args,
    schedule='@daily',
    description='Process bookings with Spark',
) as dag:
    
    read_bookings_task = PythonOperator(
        task_id='read_bookings_from_postgres',
        python_callable=read_bookings_from_postgres,
    )

    wait_for_listings_file = FileSensor(
        task_id = "wait_for_listings_file",
        fs_conn_id = "local_fs",
        filepath = "",
        poke_interval = 30,
        timeout = 600,
    )
    
    spark_job = SparkSubmitOperator(
        task_id='process_listings_and_bookings',
        application=os.path.join(DAG_FOLDER, 'bookings_per_listing_spark.py'),
        name='listings_bookings_join',
        application_args=[
            "--listings_file", "",
            "--bookings_file", "",
            "--output_path", ""
        ],
        conn_id='spark_booking',
    )


    # bookings_file = read_bookings_from_postgres()
    [read_bookings_task, wait_for_listings_file] >> spark_job