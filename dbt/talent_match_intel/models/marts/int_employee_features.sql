{{ config(schema='marts', alias='int_employee_features', materialized='view') }}

with perf as (
  select employee_id, year, rating
  from {{ ref('stg_performance_yearly') }}
),

profiles as (
  -- ambil satu baris per employee (asumsi 1 baris di profiles)
  select
    employee_id,
    iq, faxtor, pauli,
    gtq,   
    tiki   
  from {{ ref('stg_profiles_psych') }}
),

comp as (
  -- rata-rata skor kompetensi per employee per year
  select employee_id, year, avg(score)::numeric as competency_avg
  from {{ ref('stg_competencies_yearly') }}
  group by 1,2
),

last_year as (
  -- rating tahun sebelumnya untuk konteks
  select p1.employee_id, p1.year, p2.rating as rating_last_year
  from perf p1
  left join perf p2
    on p2.employee_id = p1.employee_id
   and p2.year = p1.year - 1
),

joined as (
  select
    p.employee_id::text as employee_id,   -- <— pastikan text
    p.year,
    p.rating,
    ly.rating_last_year,
    pr.iq, pr.faxtor, pr.pauli,
    pr.gtq,
    pr.tiki,
    c.competency_avg
  from {{ ref('stg_performance_yearly') }} p
  left join {{ ref('stg_profiles_psych') }} pr using (employee_id)
  left join (
    select employee_id, year, avg(score)::numeric as competency_avg
    from {{ ref('stg_competencies_yearly') }}
    group by 1,2
  ) c using (employee_id, year)
  left join (
    select p1.employee_id, p1.year, p2.rating as rating_last_year
    from {{ ref('stg_performance_yearly') }} p1
    left join {{ ref('stg_performance_yearly') }} p2
      on p2.employee_id = p1.employee_id and p2.year = p1.year - 1
  ) ly using (employee_id, year)
)
select * from joined
