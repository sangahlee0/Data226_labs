
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DATE
from USER_DB_GECKO.analytics.weather_metrics
where DATE is null



  
  
      
    ) dbt_internal_test