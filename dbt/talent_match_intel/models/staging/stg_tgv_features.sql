{{ config(materialized='view') }}

-- Performance (imputed → norm 0..1)
with perf_norm as (
  select
    employee_id,
    year,
    ((rating_imputed::double precision - 1.0) / 4.0)::double precision as performance_score_norm
  from {{ ref('int_performance_imputed') }}
),
perf_latest as (
  select distinct on (employee_id)
    employee_id, year, performance_score_norm
  from perf_norm
  order by employee_id, year desc
),

-- Competency (imputed → avg per year → norm 0..1)
comp_year_avg as (
  select
    employee_id,
    year,
    avg(score_imputed::double precision) as comp_avg_1_5
  from {{ ref('int_competencies_imputed') }}
  group by employee_id, year
),
comp_norm as (
  select
    employee_id,
    year,
    ((comp_avg_1_5 - 1.0) / 4.0)::double precision as competency_norm
  from comp_year_avg
),
comp_latest as (
  select distinct on (employee_id)
    employee_id, year, competency_norm
  from comp_norm
  order by employee_id, year desc
),

-- Cognitive (sudah norm 0..1 + flag)
cog as (
  select
    employee_id,
    has_cognitive_data,
    cognitive_norm::double precision as cognitive_norm
  from {{ ref('stg_profiles_psych_norm') }}
)

-- Final
select
  e.employee_id,
  cog.cognitive_norm,
  cog.has_cognitive_data,
  comp_latest.competency_norm,
  perf_latest.performance_score_norm as performance_norm
from {{ ref('stg_employees') }} e
left join cog         on cog.employee_id         = e.employee_id
left join comp_latest on comp_latest.employee_id = e.employee_id
left join perf_latest on perf_latest.employee_id = e.employee_id
