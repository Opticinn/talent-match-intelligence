{{ config(alias='profiles_psych', materialized='view') }}

with cleaned as (
  select
    employee_id::text as employee_id,
    nullif(trim(lower(mbti)), '') as mbti_raw,
    nullif(trim(upper(disc)), '') as disc_raw,
    iq::numeric,
    faxtor::numeric,
    pauli::numeric,

    
    gtq::int as gtq,          
    tiki::int as tiki         

  
    -- gtq_total::int as gtq_total
  from {{ source('raw','profiles_psych') }}
),
standardized as (
  select
    employee_id,
    case
      when mbti_raw in ('intj','intp','entj','entp','infj','infp','enfj','enfp',
                        'istj','isfj','estj','esfj','istp','isfp','estp','esfp')
        then upper(mbti_raw) else null end as mbti,
    disc_raw as disc,
    iq, faxtor, pauli,
    gtq,
    tiki
  from cleaned
)
select * from standardized
