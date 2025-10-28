with base as (
  -- was: ref('stg_performance_yearly_fix')
  select * from {{ ref('stg_performance_yearly') }}
),
per_year as (
  select
    year,
    percentile_cont(0.5) within group (order by rating) as med_rating_year
  from base
  where rating is not null and rating between 1 and 5
  group by 1
),
global as (
  select percentile_cont(0.5) within group (order by rating) as med_rating_global
  from base
  where rating is not null and rating between 1 and 5
),
imputed as (
  select
    b.employee_id,
    b.year,
    coalesce(
      /* keep valid */
      (case when b.rating between 1 and 5 then b.rating end),
      /* median per-year */
      py.med_rating_year,
      /* global median */
      (select med_rating_global from global),
      /* hard fallback */
      3.0
    ) as rating_imputed
  from base b
  left join per_year py using (year)
  where b.year between 2021 and 2025
)
select * from imputed
