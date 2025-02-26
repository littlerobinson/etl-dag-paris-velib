import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from s3_to_postgres import S3ToPostgresOperator

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# Globals variables
OPENWEATHERMAP_API = Variable.get("OPENWEATHERMAP_API")
S3_BUCKET_NAME = Variable.get("S3BucketName")

dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}


def _fetch_weather_data(ti):
    """Fetch current weather data from OpenWeatherMap API.

    Args:
        ti: Airflow task instance for passing data between tasks.

    The data is saved in JSON format to S3 with a timestamp.
    """
    logging.info("fetch_weather_data")
    lat = "48.866667"
    lon = "2.333333"
    headers = {
        "Accept-Encoding": "application/json",
        "User-Agent": "Chrome/126.0.0.0 ",
    }

    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&units=metric&appid={OPENWEATHERMAP_API}"
    response = requests.get(url, headers=headers)
    logging.info(f"Fetched weather data: {response.json()}")
    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_weather_data.json"
    full_path_to_file = f"/tmp/{filename}"
    with open(full_path_to_file, "w") as f:
        json.dump(response.json(), f)
    s3_hook = S3Hook(aws_conn_id="aws_citymapper")
    s3_hook.load_file(
        filename=full_path_to_file,
        key=f"jedha/citymapper_etl/{filename}",
        bucket_name=S3_BUCKET_NAME,
    )
    ti.xcom_push(key="weather_filename", value=filename)
    logging.info(f"Saved weather data to S3 with name {filename}")


def _transform_weather_data(ti):
    """Transform weather data from JSON to structured CSV format.

    Args:
        ti: Airflow task instance to retrieve the JSON filename.

    Extracts relevant data from JSON, converts it to a DataFrame
    and saves it as CSV in S3.
    """
    logging.info("transform_weather_data")
    s3_hook = S3Hook(aws_conn_id="aws_citymapper")
    weather_filename = ti.xcom_pull(
        task_ids="weather_branch.fetch_weather_data", key="weather_filename"
    )
    returned_filename = s3_hook.download_file(
        key=f"jedha/citymapper_etl/{weather_filename}",
        bucket_name=S3_BUCKET_NAME,
        local_path="/tmp",
    )

    with open(returned_filename, "r") as f:
        raw_data_json = json.load(f)

    # Create dataframe with actual weather data
    current_data = raw_data_json["current"]
    df = pd.DataFrame(
        [
            {
                "temp": current_data["temp"],
                "feels_like": current_data["feels_like"],
                "pressure": current_data["pressure"],
                "humidity": current_data["humidity"],
                "wind_speed": current_data["wind_speed"],
                "weather_description": current_data["weather"][0]["description"],
                "timestamp": datetime.fromtimestamp(current_data["dt"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            }
        ]
    )

    # Save as CSV
    csv_filename = f"/tmp/{weather_filename.split('.')[0]}.csv"
    df.to_csv(csv_filename, index=False)

    # Upload to S3
    s3_key = f"jedha/citymapper_etl/{weather_filename.split('.')[0]}.csv"
    s3_hook.load_file(
        filename=csv_filename,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    ti.xcom_push(key="weather_csv_key", value=s3_key)


def _create_weather_table():
    """Create the weather table in PostgreSQL database if it doesn't exist.

    The table contains the necessary columns to store weather data:
    temperature, feels like, pressure, humidity, wind speed, description, and timestamp.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather (
        id SERIAL PRIMARY KEY,
        temp FLOAT,
        feels_like FLOAT,
        pressure INTEGER,
        humidity INTEGER,
        wind_speed FLOAT,
        weather_description TEXT,
        timestamp TIMESTAMP,
        execution_date TIMESTAMP,
        dag_id VARCHAR(250),
        task_id VARCHAR(250)
    );
    """

    postgres_hook = PostgresHook(postgres_conn_id="postgres_citymapper")
    postgres_hook.run(create_table_sql)


def _transfer_weather_data_to_db(ti, **context):
    """Transfer weather data from S3 CSV file to PostgreSQL database.

    Args:
        ti: Airflow task instance to retrieve the CSV file key.
        context: Airflow context containing runtime variables and task instance.

    Uses S3ToPostgresOperator to load data into the weather table.
    """
    logging.info("transfer_weather_data_to_db")
    s3_key = ti.xcom_pull(
        task_ids="weather_branch.transform_weather_data", key="weather_csv_key"
    )

    s3_to_postgres = S3ToPostgresOperator(
        task_id="s3_to_postgres_weather",
        dag=dag,
        bucket=S3_BUCKET_NAME,
        key=s3_key,
        table="weather",
        postgres_conn_id="postgres_citymapper",
        aws_conn_id="aws_citymapper",
    )
    s3_to_postgres.execute(context=context)


def _fetch_station_status_data(ti):
    """Fetch current Velib station status data from the Velib Metropole API.

    Args:
        ti: Airflow task instance for passing data between tasks.

    The data is saved in JSON format to S3 with a timestamp.
    """
    logging.info("fetch_station_status_data")
    headers = {
        "Accept-Encoding": "application/json",
        "User-Agent": "Chrome/126.0.0.0 ",
    }

    url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    response = requests.get(url, headers=headers)
    logging.info(f"Fetched station status data: {response.json()}")
    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_station_status_data.json"
    full_path_to_file = f"/tmp/{filename}"
    with open(full_path_to_file, "w") as f:
        json.dump(response.json(), f)
    s3_hook = S3Hook(aws_conn_id="aws_citymapper")
    s3_hook.load_file(
        filename=full_path_to_file,
        key=f"jedha/citymapper_etl/{filename}",
        bucket_name=S3_BUCKET_NAME,
    )
    ti.xcom_push(key="station_status_filename", value=filename)
    logging.info(f"Saved station status data to S3 with name {filename}")


def _transform_station_status_data(ti):
    """Transform station status data from JSON to structured CSV format.

    Args:
        ti: Airflow task instance to retrieve the JSON filename.

    Extracts relevant station data from JSON, converts it to a DataFrame
    and saves it as CSV in S3. The data includes station ID, bike availability,
    dock availability, and station status information.
    """
    logging.info("transform_station_status_data")
    s3_hook = S3Hook(aws_conn_id="aws_citymapper")
    station_status_filename = ti.xcom_pull(
        task_ids="station_status_branch.fetch_station_status_data",
        key="station_status_filename",
    )
    returned_filename = s3_hook.download_file(
        key=f"jedha/citymapper_etl/{station_status_filename}",
        bucket_name=S3_BUCKET_NAME,
        local_path="/tmp",
    )

    with open(returned_filename, "r") as f:
        raw_data_json = json.load(f)

    # Create dataframe with actual station status data
    stations = raw_data_json["data"]["stations"]
    current_data = pd.json_normalize(stations)

    df = current_data[
        [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_returning",
            "is_renting",
            "last_reported",
        ]
    ].copy()

    df["last_reported"] = pd.to_datetime(df["last_reported"], unit="s").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Save as CSV
    csv_filename = f"/tmp/{station_status_filename.split('.')[0]}.csv"
    df.to_csv(csv_filename, index=False)

    # Upload to S3
    s3_key = f"jedha/citymapper_etl/{station_status_filename.split('.')[0]}.csv"
    s3_hook.load_file(
        filename=csv_filename,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    ti.xcom_push(key="station_status_csv_key", value=s3_key)


def _create_station_status_table():
    """Create the station_status table in PostgreSQL database if it doesn't exist.

    The table contains columns for storing Velib station status data including:
    station ID, number of available bikes and docks, installation status,
    rental/return availability status, and timestamp information.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS station_status (
        id SERIAL PRIMARY KEY,
        station_id FLOAT,
        num_bikes_available FLOAT,
        num_docks_available INTEGER,
        is_installed INTEGER,
        is_returning FLOAT,
        is_renting TEXT,
        last_reported TIMESTAMP,
        execution_date TIMESTAMP,
        dag_id VARCHAR(250),
        task_id VARCHAR(250)
    );
    """

    postgres_hook = PostgresHook(postgres_conn_id="postgres_citymapper")
    postgres_hook.run(create_table_sql)


def _transfer_station_status_data_to_db(ti, **context):
    """Transfer station status data from S3 CSV file to PostgreSQL database.

    Args:
        ti: Airflow task instance to retrieve the CSV file key.
        context: Airflow context containing runtime variables and task instance.

    Uses S3ToPostgresOperator to load the transformed station status data
    into the station_status table.
    """
    logging.info("_transfer_station_status_data_to_db")
    s3_key = ti.xcom_pull(
        task_ids="station_status_branch.transform_station_status_data",
        key="station_status_csv_key",
    )

    s3_to_postgres = S3ToPostgresOperator(
        task_id="s3_to_postgres_station_status",
        dag=dag,
        bucket=S3_BUCKET_NAME,
        key=s3_key,
        table="station_status",
        postgres_conn_id="postgres_citymapper",
        aws_conn_id="aws_citymapper",
    )
    s3_to_postgres.execute(context=context)


with DAG(
    "citymapper_dag",
    default_args=dag_default_args,
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    concurrency=2,
) as dag:
    logging.info("citymapper_dag")
    start = BashOperator(task_id="start", bash_command="echo 'Start!'")

    with TaskGroup(group_id="weather_branch") as weather_branch:
        logging.info("Enter weather_branch")
        # Fetch task and its sensor
        fetch_weather_data = PythonOperator(
            task_id="fetch_weather_data",
            python_callable=_fetch_weather_data,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        # Transform task and its sensor
        transform_weather_data = PythonOperator(
            task_id="transform_weather_data",
            python_callable=_transform_weather_data,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        # Create table task and its sensor
        create_weather_table = PythonOperator(
            task_id="create_weather_table",
            python_callable=_create_weather_table,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        # Transfer task
        transfer_weather_data = PythonOperator(
            task_id="transfer_weather_data",
            python_callable=_transfer_weather_data_to_db,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        (
            fetch_weather_data
            >> transform_weather_data
            >> create_weather_table
            >> transfer_weather_data
        )

    with TaskGroup(group_id="station_status_branch") as station_status_branch:
        logging.info("Enter station_status_branch")
        # Fetch task and its sensor
        fetch_station_status_data = PythonOperator(
            task_id="fetch_station_status_data",
            python_callable=_fetch_station_status_data,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        # Transform task and its sensor
        transform_station_status_data = PythonOperator(
            task_id="transform_station_status_data",
            python_callable=_transform_station_status_data,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        # Create table task and its sensor
        create_station_status_table = PythonOperator(
            task_id="create_station_status_table",
            python_callable=_create_station_status_table,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        # Transfer task
        transfer_station_status_data = PythonOperator(
            task_id="transfer_station_status_data",
            python_callable=_transfer_station_status_data_to_db,
            retries=3,
            retry_delay=timedelta(minutes=5),
        )

        (
            fetch_station_status_data
            >> transform_station_status_data
            >> create_station_status_table
            >> transfer_station_status_data
        )

    end = BashOperator(task_id="end", bash_command="echo 'End!'")

    start >> [weather_branch, station_status_branch] >> end
