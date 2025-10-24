-- Fail jika employee_id/tv_name/tgv_name null
select *
from {{ ref('tv_match') }}
where employee_id is null
   or tv_name is null
   or tgv_name is null
