
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CITY
from USER_DB_GECKO.analytics.weather_metrics
where CITY is null



  
  
      
    ) dbt_internal_test