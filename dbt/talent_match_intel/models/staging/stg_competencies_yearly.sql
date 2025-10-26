{{ config(materialized='view', schema='staging') }}

with base as (
  select
    cast(employee_id as text)   as employee_id,
    trim(both from pillar_code) as pillar_code,
    cast(year  as text)         as year_txt,
    cast(score as text)         as score_txt
  from {{ source('raw','competencies_yearly') }}
),

parsed as (
  select
    employee_id,
    pillar_code,

    -- YEAR: aman -> INT (terima "2024" atau "2024.0")
    case
      when year_txt is null then null
      when year_txt ~ '^\d+$'      then year_txt::int
      when year_txt ~ '^\d+\.0+$'  then regexp_replace(year_txt,'\.0+$','')::int
      else null
    end as year,

    -- SCORE: parsing aman -> INT (terima "3", "3.0", dll)
    case
      when score_txt is null then null
      when score_txt ~ '^\d+$'      then score_txt::int
      when score_txt ~ '^\d+\.0+$'  then regexp_replace(score_txt,'\.0+$','')::int
      when score_txt ~ '^\d+\.\d+$' then round((score_txt)::numeric)::int
      else null
    end as score_raw
  from base
),

cleaned as (
  select
    employee_id,
    pillar_code,
    year,
    case
      when score_raw = 6  then 5           -- ubah 6 jadi 5
      when score_raw in (0, 99) then null  -- ubah 0 dan 99 jadi NULL
      when score_raw between 1 and 5 then score_raw
      else null
    end as score
  from parsed
)

select
  employee_id,
  pillar_code,
  year,
  score
from cleaned
