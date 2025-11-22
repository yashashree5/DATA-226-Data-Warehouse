select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    forecast_date as unique_field,
    count(*) as n_records

from USER_DB_PUMA.raw.weather_history
where forecast_date is not null
group by forecast_date
having count(*) > 1



      
    ) dbt_internal_test