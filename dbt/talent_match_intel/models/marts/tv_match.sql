{{ config(alias='tv_match', materialized='view') }}

with feats as (
  select *
  from {{ ref('int_employee_features') }}
  where year = {{ var('perf_year', 2024) }}
),
b as ( select * from {{ ref('benchmarks') }} ),

long as (
  select employee_id::text as employee_id, 'iq' as tv_name, iq::numeric as tv_value, b.bench_iq as bench_value
  from feats, b
  union all
  select employee_id::text, 'faxtor', faxtor::numeric, b.bench_faxtor
  from feats, b
  union all
  select employee_id::text, 'pauli', pauli::numeric, b.bench_pauli
  from feats, b
  union all
  select employee_id::text, 'gtq', gtq::numeric, b.bench_gtq             -- hapus baris ini jika kolom gtq tidak ada
  from feats, b
  union all
  select employee_id::text, 'tiki', tiki::numeric, b.bench_tiki           -- hapus kalau tidak ada tiki
  from feats, b
  union all
  select employee_id::text, 'competency_avg', competency_avg::numeric, b.bench_competency_avg
  from feats, b
  union all
  select employee_id::text, 'rating_last_year', rating_last_year::numeric, b.bench_rating_last_year
  from feats, b
),

mapped as (
  select
    l.employee_id::text as employee_id,
    l.tv_name,
    l.tv_value::numeric as tv_value,
    l.bench_value::numeric as bench_value,
    m.tgv_name,
    m.weight::numeric,
    m.direction
  from long l
  join {{ ref('tgv_mapping') }} m on m.tv_name = l.tv_name
),

scored as (
  select
    employee_id,
    tv_name, tgv_name, weight, direction,
    tv_value, bench_value,
    case
      when tv_value is null or bench_value is null then null
      when direction = 'higher_is_better' and bench_value > 0
        then least(100::numeric, greatest(0::numeric, (tv_value / bench_value) * 100))
      when direction = 'lower_is_better' and tv_value >= 0
        then least(100::numeric, greatest(0::numeric, (bench_value / nullif(tv_value,0)) * 100))
      else null
    end::numeric as tv_match_rate
  from mapped
)
select * from scored
