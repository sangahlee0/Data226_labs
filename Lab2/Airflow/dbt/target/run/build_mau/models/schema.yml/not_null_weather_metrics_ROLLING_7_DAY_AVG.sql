
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ROLLING_7_DAY_AVG
from USER_DB_COBRA.analytics.weather_metrics
where ROLLING_7_DAY_AVG is null



  
  
      
    ) dbt_internal_test