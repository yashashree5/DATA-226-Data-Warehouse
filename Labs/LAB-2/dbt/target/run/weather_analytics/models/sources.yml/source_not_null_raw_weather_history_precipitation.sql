select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select precipitation
from USER_DB_PUMA.raw.weather_history
where precipitation is null



      
    ) dbt_internal_test