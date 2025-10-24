{{ config(alias='competencies_yearly', materialized='view') }}

with base as (
  select
    employee_id::text as employee_id,
    trim(pillar_code)::text as pillar_code,
    year::int as year,
    score::int as score
  from {{ source('raw','competencies_yearly') }}
)
select * from base
