{{ config(alias='employees', materialized='view') }}

with base as (
  select
    employee_id::text as employee_id,
    nullif(trim(fullname), '') as fullname,
    company_id::int,
    area_id::int,
    position_id::int,
    department_id::int,
    division_id::int,
    directorate_id::int,
    grade_id::int,
    education_id::int,
    major_id::int,
    years_of_service_months::int
  from {{ source('raw','employees') }}
)
select * from base
