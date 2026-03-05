
CREATE TABLE IF NOT EXISTS raw.lab1etl (
    latitude FLOAT,
    longitude FLOAT,
    date DATE,
    temp_max FLOAT,
    temp_min FLOAT,
    temp_mean FLOAT,
    precipitation FLOAT,
    weather_code VARCHAR(3),
    city VARCHAR(100),
    PRIMARY KEY (latitude, longitude, date, city)
);


CREATE SCHEMA IF NOT EXISTS ADHOC;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;


CREATE OR REPLACE VIEW adhoc.lab1city_weather_view AS
SELECT
    DATE AS DS,
    AVG(TEMP_MAX) AS TEMP_MAX,
    CITY
FROM raw.lab1etl
WHERE TEMP_MAX IS NOT NULL
GROUP BY DS, CITY;



CREATE OR REPLACE SNOWFLAKE.ML.FORECAST analytics.lab1_predict_city_weather (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'adhoc.lab1city_weather_view'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'TEMP_MAX',
    SERIES_COLNAME => 'CITY',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);


CALL analytics.lab1_predict_city_weather!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);



CREATE OR REPLACE TABLE adhoc.lab1_city_weather_forecast AS
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));


CREATE OR REPLACE TABLE analytics.lab1_city_weather_final AS
    SELECT
        CITY,
        DATE,
        TEMP_MAX AS actual_max,
        TEMP_MIN,
        TEMP_MEAN AS actual_mean,
        PRECIPITATION,
        NULL AS forecast,
        NULL AS lower_bound,
        NULL AS upper_bound
    FROM raw.lab1etl

UNION ALL

    SELECT
        REPLACE(series, '"', '') AS CITY,
        ts AS DATE,
        NULL AS actual_max,
        NULL AS TEMP_MIN,
        NULL AS actual_mean,
        NULL AS PRECIPITATION,
        forecast,
        lower_bound,
        upper_bound
    FROM adhoc.lab1_city_weather_forecast;