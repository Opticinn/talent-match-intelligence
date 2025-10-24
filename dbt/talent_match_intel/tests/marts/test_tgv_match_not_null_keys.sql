-- Fail jika employee_id/tgv_name null
select *
from {{ ref('tgv_match') }}
where employee_id is null
   or tgv_name is null
