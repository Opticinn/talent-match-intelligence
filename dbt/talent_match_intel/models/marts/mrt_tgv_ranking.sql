{{ config(materialized='view') }}

select
  employee_id,
  tgv_score,
  dense_rank() over (order by tgv_score desc) as tgv_rank
from {{ ref('tgv_match') }}
