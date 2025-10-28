{{ config(materialized='view') }}

select
  employee_id,
  year,
  rating_original,
  rating::double precision as rating,
  ((rating::double precision - 1.0) / 4.0)::double precision as performance_score_norm
from {{ ref('stg_performance_yearly') }}
where employee_id is not null
  and year is not null
  and rating between 1 and 5
