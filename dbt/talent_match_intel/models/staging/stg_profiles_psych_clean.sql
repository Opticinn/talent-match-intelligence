-- models/staging/stg_profiles_psych_clean.sql
{% set src_rel = source('raw','profiles_psych') %}

{% set has_employee_id = has_column(src_rel, 'employee_id') %}
{% set has_pauli       = has_column(src_rel, 'pauli') %}
{% set has_faxtor      = has_column(src_rel, 'faxtor') %}
{% set has_disc        = has_column(src_rel, 'disc') %}
{% set has_disc_word   = has_column(src_rel, 'disc_word') %}
{% set has_enneagram   = has_column(src_rel, 'enneagram') %}
{% set has_mbti        = has_column(src_rel, 'mbti') %}
{% set has_iq          = has_column(src_rel, 'iq') %}
{% set has_gtq1        = has_column(src_rel, 'gtq1') %}
{% set has_gtq2        = has_column(src_rel, 'gtq2') %}
{% set has_gtq3        = has_column(src_rel, 'gtq3') %}
{% set has_gtq4        = has_column(src_rel, 'gtq4') %}
{% set has_gtq5        = has_column(src_rel, 'gtq5') %}
{% set has_gtq_total   = has_column(src_rel, 'gtq_total') %}
{% set has_tiki1       = has_column(src_rel, 'tiki1') %}
{% set has_tiki2       = has_column(src_rel, 'tiki2') %}
{% set has_tiki3       = has_column(src_rel, 'tiki3') %}
{% set has_tiki4       = has_column(src_rel, 'tiki4') %}

with src as (
  select * from {{ src_rel }}
)
select
  -- KEYS
  {% if has_employee_id %} employee_id {% else %} null::text as employee_id {% endif %},

  -- PAULI & FAXTOR (→ double precision)
  {% if has_pauli %}
    case when pauli between 20 and 100 then (pauli)::double precision else null end as pauli
  {% else %}
    null::double precision as pauli
  {% endif %},

  {% if has_faxtor %}
    case when faxtor between 20 and 100 then (faxtor)::double precision else null end as faxtor
  {% else %}
    null::double precision as faxtor
  {% endif %},

  -- DISC
  {% if has_disc %}
    regexp_replace(upper(coalesce(disc,'')), '[^DISC]', '', 'g') as disc_norm,
    substr(regexp_replace(upper(coalesce(disc,'')), '[^DISC]', '', 'g'),1,1) as first_char_norm,
    substr(regexp_replace(upper(coalesce(disc,'')), '[^DISC]', '', 'g'),2,1) as second_char_norm
  {% else %}
    null::text as disc_norm,
    null::text as first_char_norm,
    null::text as second_char_norm
  {% endif %},

  {% if has_disc_word %} disc_word {% else %} null::text as disc_word {% endif %},

  -- ENNEAGRAM
  {% if has_enneagram %}
    case when enneagram between 1 and 9 then enneagram else null end as enneagram
  {% else %}
    null::int as enneagram
  {% endif %},

  -- MBTI (normalize + valid flag)
  {% if has_mbti %}
    upper(replace(replace(coalesce(mbti,''), ' ', ''), '-', '')) as mbti_norm,
    (
      upper(replace(replace(coalesce(mbti,''), ' ', ''), '-', ''))
        in ('INTJ','INTP','ENTJ','ENTP','INFJ','INFP','ENFJ','ENFP',
            'ISTJ','ISFJ','ESTJ','ESFJ','ISTP','ISFP','ESTP','ESFP')
    ) as mbti_is_valid
  {% else %}
    null::text as mbti_norm,
    null::boolean as mbti_is_valid
  {% endif %},

  -- IQ (→ double precision)
  {% if has_iq %}
    case when iq between 80 and 140 then (iq)::double precision else null end as iq
  {% else %}
    null::double precision as iq
  {% endif %},

  -- GTQ (1..10)
  {% if has_gtq1 %} case when gtq1 between 1 and 10 then gtq1 else null end as gtq1 {% else %} null::int as gtq1 {% endif %},
  {% if has_gtq2 %} case when gtq2 between 1 and 10 then gtq2 else null end as gtq2 {% else %} null::int as gtq2 {% endif %},
  {% if has_gtq3 %} case when gtq3 between 1 and 10 then gtq3 else null end as gtq3 {% else %} null::int as gtq3 {% endif %},
  {% if has_gtq4 %} case when gtq4 between 1 and 10 then gtq4 else null end as gtq4 {% else %} null::int as gtq4 {% endif %},
  {% if has_gtq5 %} case when gtq5 between 1 and 10 then gtq5 else null end as gtq5 {% else %} null::int as gtq5 {% endif %},

  {% if has_gtq_total %} gtq_total {% else %} null::int as gtq_total {% endif %},

  -- TIKI (1..10)
  {% if has_tiki1 %} case when tiki1 between 1 and 10 then tiki1 else null end as tiki1 {% else %} null::int as tiki1 {% endif %},
  {% if has_tiki2 %} case when tiki2 between 1 and 10 then tiki2 else null end as tiki2 {% else %} null::int as tiki2 {% endif %},
  {% if has_tiki3 %} case when tiki3 between 1 and 10 then tiki3 else null end as tiki3 {% else %} null::int as tiki3 {% endif %},
  {% if has_tiki4 %} case when tiki4 between 1 and 10 then tiki4 else null end as tiki4 {% else %} null::int as tiki4 {% endif %}
from src
