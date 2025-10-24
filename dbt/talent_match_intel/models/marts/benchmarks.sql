{{ config(schema='marts', alias='benchmarks', materialized='view') }}

with base as (
  select *
  from {{ ref('int_employee_features') }}
  where year = {{ var('perf_year', 2024) }}
  and rating = {{ var('high_rating', 5) }}
),

-- gunakan median agar robust terhadap outlier
bench as (
  select
    percentile_cont(0.5) within group (order by iq)          as bench_iq,
    percentile_cont(0.5) within group (order by faxtor)      as bench_faxtor,
    percentile_cont(0.5) within group (order by pauli)       as bench_pauli,
    percentile_cont(0.5) within group (order by gtq)         as bench_gtq,         
    percentile_cont(0.5) within group (order by tiki)        as bench_tiki,        
    percentile_cont(0.5) within group (order by competency_avg) as bench_competency_avg,
    percentile_cont(0.5) within group (order by rating_last_year) as bench_rating_last_year
  from base
)
select * from bench
