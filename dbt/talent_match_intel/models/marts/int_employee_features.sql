{{ config(materialized='view', schema='marts') }}

with perf as (
  select employee_id, year, rating
  from {{ ref('stg_performance_yearly') }}
),
profiles as (
  select employee_id, iq, faxtor, pauli, gtq, tiki
  from {{ ref('stg_profiles_psych') }}  -- kalau ada model stg_profiles_psych; jika belum, refer ke staging.profiles_psych
),
comp as (
  select employee_id, year, avg(score)::numeric as competency_avg
  from {{ ref('stg_competencies_yearly') }}
  group by 1,2
),
last_year as (
  select p1.employee_id, p1.year, p2.rating as rating_last_year
  from perf p1
  left join perf p2
    on p2.employee_id = p1.employee_id
   and p2.year = p1.year - 1
)
select
  p.employee_id, p.year, p.rating,
  ly.rating_last_year,
  pr.iq, pr.faxtor, pr.pauli, pr.gtq, pr.tiki,
  c.competency_avg
from perf p
left join profiles pr on pr.employee_id = p.employee_id
left join comp c       on c.employee_id  = p.employee_id and c.year = p.year
left join last_year ly on ly.employee_id = p.employee_id and ly.year = p.year
