-- models/staging/stg_dim_departments_clean.sql
select
  department_id,
  initcap(trim(name)) as name_clean
from {{ source('raw','dim_departments') }}
