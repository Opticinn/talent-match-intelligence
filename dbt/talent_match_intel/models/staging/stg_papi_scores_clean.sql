with src as (
  select * from {{ source('raw','papi_scores') }}
),
rng as (
  select
    employee_id,
    scale_code,
    case when score between 1 and 9 then score::double precision else null end as score,
    row_number() over (partition by employee_id, scale_code order by score desc nulls last) as rn
  from src
)
select employee_id, scale_code, score
from rng
where rn = 1
