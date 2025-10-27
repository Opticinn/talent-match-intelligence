{{ config(materialized='view') }}

select
  employee_id,
  pillar_code,
  year,
  score,
  (score - 1.0) / 4.0 as competency_score_norm
from {{ ref('stg_competencies_yearly') }}
where employee_id is not null
  and pillar_code is not null
  and year is not null
  and score between 1 and 5
