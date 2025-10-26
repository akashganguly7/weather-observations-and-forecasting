-- Custom test macros for weather data quality validation

{% macro test_temperature_range(model, column_name, min_temp=-50, max_temp=50) %}
  -- Test that temperature values are within reasonable range
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < {{ min_temp }} or {{ column_name }} > {{ max_temp }})
{% endmacro %}

{% macro test_precipitation_non_negative(model, column_name) %}
  -- Test that precipitation values are non-negative
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and {{ column_name }} < 0
{% endmacro %}

{% macro test_wind_speed_non_negative(model, column_name) %}
  -- Test that wind speed values are non-negative
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and {{ column_name }} < 0
{% endmacro %}

{% macro test_wind_direction_range(model, column_name) %}
  -- Test that wind direction values are between 0 and 360 degrees
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < 0 or {{ column_name }} > 360)
{% endmacro %}

{% macro test_humidity_range(model, column_name) %}
  -- Test that humidity values are between 0 and 100 percent
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < 0 or {{ column_name }} > 100)
{% endmacro %}

{% macro test_pressure_range(model, column_name, min_pressure=800, max_pressure=1100) %}
  -- Test that pressure values are within reasonable range
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < {{ min_pressure }} or {{ column_name }} > {{ max_pressure }})
{% endmacro %}

{% macro test_confidence_score_range(model, column_name) %}
  -- Test that confidence scores are between 0 and 1
  select *
  from {{ model }}
  where {{ column_name }} is not null 
    and ({{ column_name }} < 0 or {{ column_name }} > 1)
{% endmacro %}

{% macro test_data_completeness_threshold(model, column_name, threshold=0.8) %}
  -- Test that data completeness is above threshold
  select *
  from (
    select 
      count(case when {{ column_name }} is not null then 1 end)::float / count(*) as completeness_ratio
    from {{ model }}
  ) completeness_check
  where completeness_ratio < {{ threshold }}
{% endmacro %}

{% macro test_no_duplicate_records(model, unique_columns) %}
  -- Test that there are no duplicate records based on specified columns
  select *
  from (
    select 
      {{ unique_columns }},
      count(*) as record_count
    from {{ model }}
    group by {{ unique_columns }}
    having count(*) > 1
  ) duplicates
{% endmacro %}

{% macro test_timestamp_continuity(model, timestamp_column, time_interval='1 hour') %}
  -- Test that timestamps are continuous (no gaps larger than specified interval)
  with timestamp_gaps as (
    select 
      {{ timestamp_column }},
      lag({{ timestamp_column }}) over (order by {{ timestamp_column }}) as prev_timestamp,
      {{ timestamp_column }} - lag({{ timestamp_column }}) over (order by {{ timestamp_column }}) as time_diff
    from {{ model }}
    order by {{ timestamp_column }}
  )
  select *
  from timestamp_gaps
  where time_diff > interval '{{ time_interval }}'
{% endmacro %}
