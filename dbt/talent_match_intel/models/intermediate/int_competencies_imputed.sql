with base as (
  -- was: ref('stg_competencies_yearly_fix')
  select * from {{ ref('stg_competencies_yearly_clean') }}
),
per_ctx as (
  select
    pillar_code,
    year,
    percentile_cont(0.5) within group (order by score) as med_score_ctx
  from base
  where score is not null and score between 1 and 5
  group by 1,2
),
global as (
  select percentile_cont(0.5) within group (order by score) as med_score_global
  from base
  where score is not null and score between 1 and 5
),
imputed as (
  select
    b.employee_id,
    b.pillar_code,
    b.year,
    coalesce(
      (case when b.score between 1 and 5 then b.score end),
      pc.med_score_ctx,
      (select med_score_global from global),
      3.0
    ) as score_imputed
  from base b
  left join per_ctx pc
    on pc.pillar_code = b.pillar_code and pc.year = b.year
  where b.year between 2021 and 2025
)
select * from imputed
