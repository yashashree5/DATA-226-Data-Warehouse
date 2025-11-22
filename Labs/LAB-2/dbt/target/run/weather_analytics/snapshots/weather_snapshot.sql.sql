
      
  
    

        create or replace transient table USER_DB_PUMA.snapshot.weather_snapshot
         as
        (

    select *,
        md5(coalesce(cast(forecast_date as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))) as dbt_valid_to
    from (
        



SELECT * FROM USER_DB_PUMA.raw.weather_history

    ) sbq



        );
      
  
  