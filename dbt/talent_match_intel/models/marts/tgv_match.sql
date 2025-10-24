{{ config(alias='tgv_match', materialized='view') }}

with base as (
  select
    employee_id::text as employee_id,
    tgv_name,
    avg(tv_match_rate)::numeric as tgv_match_rate
  from {{ ref('tv_match') }}
  group by 1,2
)
select * from base
