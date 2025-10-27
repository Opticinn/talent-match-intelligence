{{ config(materialized='view') }}

with base as (
  select
    p.employee_id,
    e.department_id,
    p.iq::numeric      as iq,
    p.gtq::numeric     as gtq,
    p.tiki::numeric    as tiki,
    p.faxtor::numeric  as faxtor,
    p.pauli::numeric   as pauli,
    case when p.iq is not null or p.gtq is not null then true else false end as has_cognitive_data
  from {{ source('raw','profiles_psych') }} p
  join {{ source('raw','employees') }} e using (employee_id)
  where p.employee_id is not null
),
valid_dept as (
  select department_id
  from base
  where has_cognitive_data
  group by department_id
),
dept_med as (
  select
    department_id,
    percentile_cont(0.5) within group (order by iq)      as med_iq,
    percentile_cont(0.5) within group (order by gtq)     as med_gtq,
    percentile_cont(0.5) within group (order by tiki)    as med_tiki,
    percentile_cont(0.5) within group (order by faxtor)  as med_faxtor,
    percentile_cont(0.5) within group (order by pauli)   as med_pauli
  from base
  where department_id in (select department_id from valid_dept)
  group by department_id
),
glob_med as (
  select
    percentile_cont(0.5) within group (order by iq)      as gmed_iq,
    percentile_cont(0.5) within group (order by gtq)     as gmed_gtq,
    percentile_cont(0.5) within group (order by tiki)    as gmed_tiki,
    percentile_cont(0.5) within group (order by faxtor)  as gmed_faxtor,
    percentile_cont(0.5) within group (order by pauli)   as gmed_pauli
  from base
  where has_cognitive_data
),
impute as (
  select
    b.employee_id,
    b.department_id,
    b.has_cognitive_data,
    coalesce(b.iq,     d.med_iq,     g.gmed_iq    ) as iq_filled,
    coalesce(b.gtq,    d.med_gtq,    g.gmed_gtq   ) as gtq_filled,
    coalesce(b.tiki,   d.med_tiki,   g.gmed_tiki  ) as tiki_filled,
    coalesce(b.faxtor, d.med_faxtor, g.gmed_faxtor) as faxtor_filled,
    coalesce(b.pauli,  d.med_pauli,  g.gmed_pauli ) as pauli_filled
  from base b
  left join dept_med d on d.department_id = b.department_id
  cross join glob_med g
)
select
  employee_id,
  department_id,
  has_cognitive_data,
  iq_filled,
  gtq_filled,
  tiki_filled,
  faxtor_filled,
  pauli_filled,
  (coalesce(iq_filled,0) + coalesce(gtq_filled,0) + coalesce(tiki_filled,0)
   + coalesce(faxtor_filled,0) + coalesce(pauli_filled,0)) / 5.0 as cognitive_index
from impute
