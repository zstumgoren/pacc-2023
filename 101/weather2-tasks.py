from datetime import timedelta

import httpx
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


@task(
    persist_result=True,
    result_serializer="json",
    retries=2,
    retry_delay_seconds=[1, 10]
)
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    logger = get_run_logger()
    logger.info(f"Current temp in Celsius: {most_recent_temp}")
    return most_recent_temp


@task(
    persist_result=True,
    result_serializer="json",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=10)
)
def convert_celsius_to_farenheit(temp: float):
    converted_temp = temp * 1.8 + 32
    logger = get_run_logger()
    logger.info(f"Current temp in Farenheit: {converted_temp}")
    return converted_temp

@task
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"


@flow
def pipeline(lat: float, lon: float):
    temp = fetch_weather(lat, lon)
    faren = convert_celsius_to_farenheit(temp)
    print(f"SF temp is {faren}")
    result = save_weather(faren)
    print("Wrote temp to weather.csv")
    return faren


if __name__ == "__main__":
    sf = (37.77, -122.4)
    #pipeline(38.9, -77.0)
    pipeline(*sf)
