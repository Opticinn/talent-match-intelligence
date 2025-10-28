-- models/staging/stg_competencies_yearly_clean.sql
with src as (
  select * from {{ source('raw','competencies_yearly') }}
),
dim as (
  select pillar_code from {{ source('raw','dim_competency_pillars') }}
),
-- Tandai nilai score yang benar2 numerik (mis. "4" atau "4.0")
typed as (
  select
    c.employee_id,
    c.pillar_code,
    c.year,
    case
      when c.score ~ '^\s*[0-9]+(\.[0-9]+)?\s*$' then cast(trim(c.score) as numeric)
      else null
    end as score_num
  from src as c
)
select
  t.employee_id,
  t.pillar_code,
  t.year,
  case when t.score_num between 1 and 5 then t.score_num else null end as score
from typed t
join dim d
  on t.pillar_code = d.pillar_code
where t.year between 2021 and 2025
