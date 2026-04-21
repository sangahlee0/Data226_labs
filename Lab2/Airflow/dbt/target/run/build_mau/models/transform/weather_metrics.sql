
  
    

        create or replace transient table USER_DB_GECKO.analytics.weather_metrics
         as
        (

SELECT 
    CITY, 
    DATE, 
    TEMP_MAX,
    PRECIPITATION,

    -- 7-Day Moving Average of Maximum Temperature
    AVG(TEMP_MAX) OVER (
        PARTITION BY CITY 
        ORDER BY DATE 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_avg,

    -- Rolling Rainfall (7-day total of precipitation)
    SUM(PRECIPITATION) OVER (
        PARTITION BY CITY 
        ORDER BY DATE 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7_day_rainfall,
    
    -- Temperature Anomaly
    TEMP_MAX - AVG(TEMP_MAX) OVER (PARTITION BY CITY) AS temp_anomaly,

    -- Days with 0 rain to use for visualization
    CASE WHEN PRECIPITATION = 0 THEN 1 ELSE 0 END AS is_dry_day

FROM USER_DB_GECKO.raw.lab1etl
        );
      
  