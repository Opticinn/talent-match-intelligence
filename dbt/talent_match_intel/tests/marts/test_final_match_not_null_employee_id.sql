-- Fail jika ada employee_id null
select *
from {{ ref('final_match') }}
where employee_id is null
