{{ config(materialized='view') }}

with perf_latest as (
  select distinct on (employee_id)
    employee_id, year, performance_score_norm
  from {{ ref('stg_performance_yearly_norm') }}
  order by employee_id, year desc
),
comp_year_avg as (
  select employee_id, year, avg(competency_score_norm) as competency_norm
  from {{ ref('stg_competencies_yearly_norm') }}
  group by employee_id, year
),
comp_latest as (
  select distinct on (employee_id)
    employee_id, year, competency_norm
  from comp_year_avg
  order by employee_id, year desc
),
cog as (
  select employee_id, has_cognitive_data, cognitive_norm
  from {{ ref('stg_profiles_psych_norm') }}
)
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
