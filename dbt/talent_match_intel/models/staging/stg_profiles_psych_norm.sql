{{ config(materialized='view') }}

with src as (
  select
    employee_id,
    has_cognitive_data,
    cognitive_index
  from {{ ref('stg_profiles_psych_clean') }}
  where cognitive_index is not null
),
bounds as (
  select min(cognitive_index) as mn, max(cognitive_index) as mx
  from src
)
select
  c.employee_id,
  c.has_cognitive_data,
  c.cognitive_index,
  case
    when b.mx = b.mn then 0.5
    else (c.cognitive_index - b.mn) / nullif(b.mx - b.mn, 0)
  end as cognitive_norm
from {{ ref('stg_profiles_psych_clean') }} c
left join bounds b on true
