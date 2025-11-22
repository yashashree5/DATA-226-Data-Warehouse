{% snapshot weather_snapshot %}

{{
    config(
      target_schema='snapshot',
      unique_key='forecast_date',
      strategy='check',
      check_cols='all'
    )
}}

SELECT * FROM {{ source('raw', 'weather_history') }}

{% endsnapshot %}