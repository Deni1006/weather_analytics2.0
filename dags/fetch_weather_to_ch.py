from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import os
import requests
from clickhouse_driver import Client

# Города для загрузки (можешь добавить свои)
CITIES = [
    {"city_id": 1, "city": "Amsterdam", "lat": 52.3676, "lon": 4.9041},
    {"city_id": 2, "city": "Berlin",    "lat": 52.5200, "lon": 13.4050},
]

def fetch_and_load_to_clickhouse(**_):
    # Подключение к ClickHouse
    client = Client(
        host=os.environ["CH_HOST"],
        port=int(os.environ.get("CH_PORT", "9440")),
        user=os.environ["CH_USER"],
        password=os.environ["CH_PASSWORD"],
        database=os.environ.get("CH_DATABASE", "weather"),
        secure=True,  # TLS для ClickHouse Cloud
    )

    rows = []
    for c in CITIES:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={c['lat']}&longitude={c['lon']}"
            "&hourly=temperature_2m,relativehumidity_2m,windspeed_10m"
            "&past_days=7&forecast_days=1"
            "&windspeed_unit=ms"
            "&timezone=UTC"
        )
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        h = data["hourly"]
        for t, temp, hum, wind in zip(
            h["time"], h["temperature_2m"], h["relativehumidity_2m"], h["windspeed_10m"]
        ):
            rows.append((
                t,
                c["city_id"],
                c["city"],
                c["lat"],
                c["lon"],
                float(temp) if temp is not None else None,
                float(hum) if hum is not None else None,
                float(wind) if wind is not None else None,
            ))

    if rows:
        client.execute(
            """
            INSERT INTO weather.weather_hourly
            (ts, city_id, city, lat, lon, temp_c, rel_humidity, wind_speed_ms)
            VALUES
            """,
            rows
        )

default_args = {
    "owner": "astro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_weather_to_clickhouse",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["weather", "clickhouse"],
) as dag:
    PythonOperator(
        task_id="fetch_and_load",
        python_callable=fetch_and_load_to_clickhouse,
    )
