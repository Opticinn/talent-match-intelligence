-- models/qa/qa_grid_eval_2yr.sql
{{ config(materialized='table', schema='qa') }}

with grid as (
  select g1/100.0 as w_cog, g2/100.0 as w_comp, g3/100.0 as w_perf
  from generate_series(0,100,5) g1,
       generate_series(0,100,5) g2,
       generate_series(0,100,5) g3
  where g1 + g2 + g3 = 100
),
yr as (
  select max(year) as y2, max(year)-1 as y1 from marts.int_employee_features
),
yr_weight as (
  select (select y2 from yr) as year, 1.0::numeric as w
  union all
  select (select y1 from yr), 0.7::numeric
),
labels as (
  select i.employee_id, i.year,
         case when i.rating >= 5 then 1 else 0 end as y
  from marts.int_employee_features i
  join yr_weight using (year)
),
b as (
  select bench_iq::numeric               as bench_iq,
         bench_faxtor::numeric           as bench_faxtor,
         bench_pauli::numeric            as bench_pauli,
         bench_gtq::numeric              as bench_gtq,
         bench_tiki::numeric             as bench_tiki,
         bench_competency_avg::numeric   as bench_competency_avg,
         bench_rating_last_year::numeric as bench_rating_last_year
  from marts.benchmarks
  limit 1
),
feats as (
  select f.employee_id, f.year,
         f.iq::numeric               as iq,
         f.faxtor::numeric           as faxtor,
         f.pauli::numeric            as pauli,
         f.gtq::numeric              as gtq,
         f.tiki::numeric             as tiki,
         f.competency_avg::numeric   as competency_avg,
         f.rating_last_year::numeric as rating_last_year
  from marts.int_employee_features f
  join yr_weight using (year)
),
tv_long as (
  select employee_id, year, 'iq'::text               as tv_name, iq               as tv_value, (select bench_iq               from b) as bench_value from feats
  union all select employee_id, year, 'faxtor'            , faxtor           , (select bench_faxtor          from b) as bench_value from feats
  union all select employee_id, year, 'pauli'             , pauli            , (select bench_pauli           from b) as bench_value from feats
  union all select employee_id, year, 'gtq'               , gtq              , (select bench_gtq             from b) as bench_value from feats
  union all select employee_id, year, 'tiki'              , tiki             , (select bench_tiki            from b) as bench_value from feats
  union all select employee_id, year, 'competency_avg'    , competency_avg   , (select bench_competency_avg  from b) as bench_value from feats
  union all select employee_id, year, 'rating_last_year'  , rating_last_year , (select bench_rating_last_year from b) as bench_value from feats
),
mapped as (
  select
    l.employee_id,
    l.year,
    l.tv_name,
    l.tv_value::numeric     as tv_value,
    l.bench_value::numeric  as bench_value,
    m.tgv_name,
    m.weight::numeric       as weight,
    m.direction::text       as direction
  from tv_long l
  join staging.tgv_mapping m
    on m.tv_name = l.tv_name
),
tv_scored as (
  select
    employee_id, year, tv_name, tgv_name, weight, direction, tv_value, bench_value,
    case
      when tv_value is null or bench_value is null then null::numeric
      when direction = 'higher_is_better' and bench_value > 0
        then least(100::numeric, greatest(0::numeric, (tv_value/bench_value)*100))
      when direction = 'lower_is_better'  and tv_value >= 0
        then least(100::numeric, greatest(0::numeric, (bench_value/nullif(tv_value,0))*100))
      else null::numeric
    end as tv_match_rate
  from mapped
),
tgv_scores as (
  select employee_id, year, tgv_name,
         avg(tv_match_rate)::numeric as tgv_match_rate
  from tv_scored
  group by 1,2,3
),
tgv_pivot as (
  select
    employee_id, year,
    max(case when tgv_name='Cognitive'   then tgv_match_rate end)::numeric   as cog,
    max(case when tgv_name='Competency'  then tgv_match_rate end)::numeric   as comp,
    max(case when tgv_name='Performance' then tgv_match_rate end)::numeric   as perf
  from tgv_scores
  group by 1,2
),
yr_join as (
  select p.*, y.w
  from tgv_pivot p
  join yr_weight y using (year)
),
final_scores as (
  select
    l.employee_id, l.year, y.w,
    g.w_cog, g.w_comp, g.w_perf,
    (coalesce(p.cog,0)*g.w_cog + coalesce(p.comp,0)*g.w_comp + coalesce(p.perf,0)*g.w_perf)::numeric as final_score,
    l.y as label
  from labels l
  join yr_join p using (employee_id, year)
  join yr_weight y using (year)
  cross join grid g
),
eval as (
  select
    w_cog, w_comp, w_perf,
    corr(final_score * w, (label::numeric) * w) as corr_recency_weighted,
    count(*) as obs
  from final_scores
  group by 1,2,3
)
select *
from eval
order by corr_recency_weighted desc nulls last, obs desc
