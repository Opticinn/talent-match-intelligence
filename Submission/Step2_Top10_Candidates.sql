-- Top-10 kandidat per vacancy

with
  emp_final as (
    -- Ambil 1 nilai final per kandidat
    select
      job_vacancy_id,
      employee_id,
      max(final_match_rate) as final_match_rate
    from
      marts.mrt_talent_match_all
    group by
      job_vacancy_id,
      employee_id
  ),
  rnk as (
    select
      ef.job_vacancy_id,
      ef.employee_id,
      ef.final_match_rate,
      row_number() over (
        partition by
          ef.job_vacancy_id
        order by
          ef.final_match_rate desc
      ) as rn
    from
      emp_final ef
  )
select
  r.job_vacancy_id,
  r.employee_id,
  a.fullname,
  a.directorate,
  a.role,
  a.grade,
  a.years_of_service_months,
  round(
    least(greatest(r.final_match_rate, 0), 100)::numeric,
    2
  ) as final_match_rate, -- clamp 0..100
  r.rn
from
  rnk r
  join (
    select distinct
      employee_id,
      fullname,
      directorate,
      role,
      grade,
      years_of_service_months
    from
      marts.mrt_talent_match_all
  ) a using (employee_id)
where
  r.rn <= 10
order by
  r.job_vacancy_id,
  r.rn;