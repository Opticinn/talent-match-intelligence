-- Fail jika ada duplikat employee_id
with dups as (
  select employee_id, count(*) cnt
  from {{ ref('final_match') }}
  group by 1
  having count(*) > 1
)
select * from dups
