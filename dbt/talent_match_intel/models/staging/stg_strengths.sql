{{ config(alias='strengths', materialized='view') }}


with base as (
  select
    employee_id::text as employee_id,
    rank::int as rank,
    nullif(trim(theme), '') as theme
  from {{ source('raw','strengths') }}
)
select * from base
