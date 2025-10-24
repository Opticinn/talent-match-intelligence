{{ config(alias='papi_scores', materialized='view') }}

with base as (
  select
    employee_id::text as employee_id,
    trim(scale_code)::text as scale_code,
    score::int as score
  from {{ source('raw','papi_scores') }}
)
select * from base
