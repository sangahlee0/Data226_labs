# Weather Forecasting Data Pipeline (Lab 1)

## Overview

This project implements an end-to-end multi-city weather forecasting system using:

- **Open-Meteo API** (data source)
- **Apache Airflow** (orchestration)
- **Snowflake** (data warehouse + ML forecasting)

The system automatically retrieves historical weather data, trains a time-series forecasting model, and generates a 7-day maximum temperature forecast for multiple cities.

---

## Architecture

Open-Meteo API  
→ Airflow ETL DAG  
→ Snowflake RAW Table  
→ Snowflake ML Forecast Model  
→ Forecast Table  
→ Final Analytics Table  

Two Airflow DAGs are used:

- `Lab1_ETL.py` – Data ingestion (Extract → Transform → Load)
- `Lab1_forecast.py` – ML training and forecasting (Train → Predict)

---

## Repository Structure
- Lab1_ETL.py # ETL pipeline
- Lab1_forecast.py # ML forecasting pipeline
- lab_2.sql # Unified SQL script
- README.md # Project documentation


---

## Pipeline Details

### DAG 1: ETL Pipeline (`Lab1_ETL.py`)
- Fetches last 60 days of daily weather data
- Supports multiple cities via Airflow Variable `MULTI_LOCATIONS`
- Loads data into: `raw.lab1etl`

- Uses SQL transactions (`BEGIN / COMMIT / ROLLBACK`)

---

### DAG 2: ML Forecasting Pipeline (`Lab1_forecast.py`)
- Creates training view: `adhoc.lab1city_weather_view`

- Creates Snowflake ML model: `analytics.lab1_predict_city_weather`

- Generates 7-day forecast
- Creates final analytics table: `analytics.lab1_city_weather_final`

- Final table unions historical and forecast data

---

## Snowflake Tables

- `raw.lab1etl` — Historical weather data
- `adhoc.lab1city_weather_view` — Training view
- `adhoc.lab1_city_weather_forecast` — Forecast output
- `analytics.lab1_city_weather_final` — Final union table

---

## Configuration

### Airflow Connection
- Connection ID: `snowflake_con`

### Airflow Variable
`MULTI_LOCATIONS` (JSON format):

```json
[
{"city": "Fremont", "lat": 37.5485, "lon": -121.9886},
{"city": "Cupertino", "lat": 37.3229, "lon": -122.0322},
{"city": "San Francisco", "lat": 37.7749, "lon": -122.4194}
]