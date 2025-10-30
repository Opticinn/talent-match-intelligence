{{ config(materialized='view', schema='intermediate') }}


with w as (
  select 0.05::numeric as w_cog, 0.90::numeric as w_comp, 0.05::numeric as w_perf
),
f as (
  select * from {{ ref('stg_tgv_features') }}
),
avail as (
  select
    f.*,
    (case when has_cognitive_data and cognitive_norm is not null then 1 else 0 end) as has_cog,
    (case when competency_norm is not null then 1 else 0 end) as has_comp,
    (case when performance_norm is not null then 1 else 0 end) as has_perf
  from f
),
adj as (
  select
    a.*,
    (case when has_cog=1 then w.w_cog else 0 end +
     case when has_comp=1 then w.w_comp else 0 end +z
     case when has_perf=1 then w.w_perf else 0 end) as available_sum
  from avail a cross join w
)
select
  employee_id,
  cognitive_norm,
  competency_norm,
  performance_norm,
  has_cognitive_data,
  (coalesce(cognitive_norm,0)  * (case when has_cog=1  then w.w_cog  else 0 end) / nullif(available_sum,0)) as weighted_cog,
  (coalesce(competency_norm,0) * (case when has_comp=1 then w.w_comp else 0 end) / nullif(available_sum,0)) as weighted_comp,
  (coalesce(performance_norm,0)* (case when has_perf=1 then w.w_perf else 0 end) / nullif(available_sum,0)) as weighted_perf,
  ((coalesce(cognitive_norm,0)  * (case when has_cog=1  then w.w_cog  else 0 end) +
    coalesce(competency_norm,0) * (case when has_comp=1 then w.w_comp else 0 end) +
    coalesce(performance_norm,0)* (case when has_perf=1 then w.w_perf else 0 end)
   ) / nullif(available_sum,0)) as tgv_score
from adj cross join w
