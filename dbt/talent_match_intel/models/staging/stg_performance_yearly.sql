create or replace view staging.performance_yearly as
with base as (
  select
    employee_id::text                 as employee_id,
    (year)::text                      as year_txt,
    (rating)::text                    as rating_txt
  from raw.performance_yearly
),
parsed as (
  select
    employee_id,

    -- YEAR: aman ke INTEGER (boleh "2024" atau "2024.0")
    case
      when year_txt is null then null
      when year_txt ~ '^\d+$'      then year_txt::int
      when year_txt ~ '^\d+\.0+$'  then regexp_replace(year_txt,'\.0+$','')::int
      else null  -- kasus lain: NULL saja (daripada error)
    end as year,

    -- Simpan rating original (teks) untuk debug
    rating_txt as rating_original,

    -- RATING: parse ke INT aman (boleh "3" atau "3.0", desimal lain dibulatkan)
    case
      when rating_txt is null then null
      when rating_txt ~ '^\d+$'      then rating_txt::int
      when rating_txt ~ '^\d+\.0+$'  then regexp_replace(rating_txt,'\.0+$','')::int
      when rating_txt ~ '^\d+\.\d+$' then round((rating_txt)::numeric)::int
      else null
    end as rating_int_raw
  from base
),
cleaned as (
  select
    employee_id,
    year,
    rating_original,

    -- Terapkan aturan bisnis ke rating (tanpa bikin error)
    case
      when rating_int_raw is null then null
      when rating_int_raw = 6  then 5
      when rating_int_raw in (0,99) then null
      when rating_int_raw between 1 and 5 then rating_int_raw
      else null
    end as rating
  from parsed
)
select employee_id, year, rating_original, rating
from cleaned
