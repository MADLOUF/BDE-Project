from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

URL = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
DATASET_ID = "mydataset"
TABLE_NAME= "velibstatus"
GCS_BUCKET_NAME= "europe-central2-bdeproject-fdf3e23c-bucket"
FILE_NAME= "data.csv"

default_args = {
    "owner": "me",
    "start_date": datetime(2022, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "json_to_csv_to_bigquery",
    default_args=default_args,
    description="Load JSON data from a URL, convert it to CSV, and load it into BigQuery",
    schedule_interval=timedelta(hours=1),
)


def fetch_json(**kwargs):
    # Make an HTTP GET request to the URL
    response = requests.get(URL)
    data = response.json()

    # Save the data to a JSON file
    with open("data.json", "w") as f:
        f.write(response.text)

fetch_json_task = PythonOperator(
    task_id="fetch_json",
    python_callable=fetch_json,
    dag=dag,
)
def json_to_csv(**kwargs):
    # Load the JSON data into a Pandas DataFrame
    df = pd.read_json("data.json")

    # Select the "stations" column from the DataFrame
    stations = df["data"]["stations"]

    # Create a new DataFrame from the "stations" column
    new_df = pd.DataFrame(stations)

    # Split the "num_bikes_available_types" column into two separate columns
    new_df[["num_bikes_available_mechanical", "num_bikes_available_ebike"]] = pd.DataFrame(
        new_df["num_bikes_available_types"].tolist(), index=new_df.index
    )

    # Extract the numeric values from the "num_bikes_available_mechanical" and "num_bikes_available_ebike" columns
    new_df["num_bikes_available_mechanical"] = new_df["num_bikes_available_mechanical"].apply(
        lambda x: x["mechanical"]
    )
    new_df["num_bikes_available_ebike"] = new_df["num_bikes_available_ebike"].apply(
        lambda x: x["ebike"]
    )

    # Remove the "num_bikes_available_types" column from the DataFrame
    new_df = new_df.drop("num_bikes_available_types", axis=1)

    # Save the resulting DataFrame as a CSV file
    new_df.to_csv("data.csv", index=False)

json_to_csv_task = PythonOperator(
    task_id="json_to_csv",
    python_callable=json_to_csv,
    dag=dag,
)



load_task = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"bq load --source_format=CSV {DATASET_ID}.{TABLE_NAME} gs://{GCS_BUCKET_NAME}/data/{FILE_NAME}",
        dag=dag,
    )

fetch_json_task >> json_to_csv_task >> load_task
