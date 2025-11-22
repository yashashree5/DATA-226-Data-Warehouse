select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select max_temp
from USER_DB_PUMA.raw.weather_history
where max_temp is null



      
    ) dbt_internal_test