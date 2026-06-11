from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
import random
import os, csv

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

def generate_bookings(**context):
    execution_date = context["data_interval_start"]
    ds = context["ds"]
    time_str = execution_date.strftime("%Y-%m-%d_%H%M")
    
    file_path = ""
    
    num_bookings = random.randint(30, 50)
    bookings = []
    for _ in range(num_bookings):
        booking = {
            "booking_id": random.randint(1000, 5000),
            "listing_id": random.choice([13913, 17402, 24328, 33332, 116268, 117203, 127652, 127860]),
            "user_id": random.randint(1000, 5000),
            "booking_time": time_str,
            "status": random.choice(["confirmed", "cancelled", "pending"])
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
            writer.writerow(booking)
    
    print(f"generated bookings data written to {file_path}")
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
    
    generate_bookings_task = PythonOperator(
        task_id='generate_bookings',
        python_callable=generate_bookings,
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
    
    # generate_bookings_task >> spark_job
    # wait_for_listings_file >> spark_job

    # generate_bookings_task >> wait_for_listings_file >> spark_job
    [generate_bookings_task, wait_for_listings_file] >> spark_job