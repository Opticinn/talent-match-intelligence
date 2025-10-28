with base as (
  -- was: ref('stg_papi_scores_fix')
  select
    employee_id,
    upper(trim(scale_code)) as scale_code,
    score
  from {{ ref('stg_papi_scores') }}
),
per_scale as (
  select
    scale_code,
    percentile_cont(0.5) within group (order by score) as med_score_scale
  from base
  where score is not null and score between 1 and 9
  group by 1
),
global as (
  select percentile_cont(0.5) within group (order by score) as med_score_global
  from base
  where score is not null and score between 1 and 9
),
imputed as (
  select
    b.employee_id,
    b.scale_code,
    coalesce(
      (case when b.score between 1 and 9 then b.score end),
      ps.med_score_scale,
      (select med_score_global from global),
      5.0
    ) as score_imputed
  from base b
  left join per_scale ps using (scale_code)
)
select * from imputed
