create or replace view staging.competencies_yearly as
with base as (
  select
    employee_id::text as employee_id,
    trim(both from pillar_code) as pillar_code,
    (year)::text   as year_txt,
    (score)::text  as score_txt
  from raw.competencies_yearly
),
cleaned as (
  select
    employee_id,
    pillar_code,

    -- YEAR aman → INT
    case
      when year_txt is null then null
      when year_txt ~ '^\d+$'      then year_txt::int
      when year_txt ~ '^\d+\.0+$'  then regexp_replace(year_txt,'\.0+$','')::int
      else null
    end as year,

    -- SCORE aman → INT (kalau “3.0” jadi 3, kalau desimal lain dibulatkan)
    case
      when score_txt is null then null
      when score_txt ~ '^\d+$'      then score_txt::int
      when score_txt ~ '^\d+\.0+$'  then regexp_replace(score_txt,'\.0+$','')::int
      when score_txt ~ '^\d+\.\d+$' then round((score_txt)::numeric)::int
      else null
    end as score
  from base
)
select employee_id, pillar_code, year, score
from cleaned
