with src as (select * from {{ source('raw','strengths') }}),
rng as (
  select
    employee_id,
    case when rank between 1 and 14 then rank else null end as rank,
    trim(theme) as theme,
    row_number() over (partition by employee_id, rank order by theme) as rn
  from src
),
dedup as (
  select employee_id, rank, theme from rng where rn = 1
),
agg as (
  select employee_id, count(*) filter (where rank is not null) as n_ranks
  from dedup group by 1
)
select d.*, (a.n_ranks = 14) as has_full_strengths
from dedup d
left join agg a using (employee_id)
