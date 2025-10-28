{{ config(materialized='view', schema='staging') }}

with base as (
  select
    cast(employee_id as text) as employee_id,
    cast(year as text)        as year_txt,
    cast(rating as text)      as rating_txt
  from {{ source('raw','performance_yearly') }}
),
parsed as (
  select
    employee_id,

    -- YEAR -> INT (terima '2024' atau '2024.0')
    case
      when year_txt is null then null
      when year_txt ~ '^\d+$'      then year_txt::int
      when year_txt ~ '^\d+\.0+$'  then regexp_replace(year_txt,'\.0+$','')::int
      else null
    end as year,

    -- Simpan rating original untuk debug
    rating_txt as rating_original,

    -- RATING -> DOUBLE (terima '3', '3.0', desimal lain dibulatkan ke int dulu)
    case
      when rating_txt is null then null
      when rating_txt ~ '^\d+$'      then (rating_txt::int)::double precision
      when rating_txt ~ '^\d+\.0+$'  then (regexp_replace(rating_txt,'\.0+$','')::int)::double precision
      when rating_txt ~ '^\d+\.\d+$' then (round((rating_txt)::numeric)::int)::double precision
      else null
    end as rating_dp_raw
  from base
),
cleaned as (
  select
    employee_id,
    year,
    rating_original,
    case
      when rating_dp_raw is null then null
      when rating_dp_raw = 6 then 5::double precision
      when rating_dp_raw in (0, 99) then null
      when rating_dp_raw between 1 and 5 then rating_dp_raw
      else null
    end as rating
  from parsed
)
select
  employee_id,
  year,
  rating_original,
  rating
from cleaned
