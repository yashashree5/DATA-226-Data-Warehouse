select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select wind_speed
from USER_DB_PUMA.raw.weather_history
where wind_speed is null



      
    ) dbt_internal_test