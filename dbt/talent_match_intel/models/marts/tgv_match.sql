{{ config(materialized='view') }}

select
  employee_id,
  cognitive_norm,
  competency_norm,
  performance_norm,
  has_cognitive_data,
  weighted_cog,
  weighted_comp,
  weighted_perf,
  tgv_score
from {{ ref('int_tgv_score') }}
