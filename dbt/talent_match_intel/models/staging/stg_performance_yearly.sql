{{ config(alias='performance_yearly', materialized='view') }}

with base as (
  select
    employee_id::text as employee_id,
    year::int as year,
    rating::int as rating
  from {{ source('raw','performance_yearly') }}
)
select * from base
