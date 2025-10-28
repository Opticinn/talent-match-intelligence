with src as (
  select * from {{ ref('stg_profiles_psych_enrich') }} -- ganti dari _clean ke _enrich
),
norm as (
  select
    employee_id,
    mbti_norm,
    disc_norm,

    -- raw
    iq, pauli, faxtor, gtq1, gtq2, gtq3, gtq4, gtq5, gtq_total, tiki1, tiki2, tiki3, tiki4,

    -- flag kognitif
    (
         (iq     is not null)
      or (pauli  is not null)
      or (faxtor is not null)
      or (gtq1   is not null) or (gtq2 is not null) or (gtq3 is not null) or (gtq4 is not null) or (gtq5 is not null)
      or (tiki1  is not null) or (tiki2 is not null) or (tiki3 is not null) or (tiki4 is not null)
    ) as has_cognitive_data,

    -- minmax
    case when iq     is not null then greatest(0, least(1, (iq     - 80.0) / 60.0)) end as iq_minmax,
    case when pauli  is not null then greatest(0, least(1, (pauli  - 20.0) / 80.0)) end as pauli_minmax,
    case when faxtor is not null then greatest(0, least(1, (faxtor - 20.0) / 80.0)) end as faxtor_minmax,

    case when gtq1 is not null then (gtq1 - 1.0)/9.0 end as gtq1_mm,
    case when gtq2 is not null then (gtq2 - 1.0)/9.0 end as gtq2_mm,
    case when gtq3 is not null then (gtq3 - 1.0)/9.0 end as gtq3_mm,
    case when gtq4 is not null then (gtq4 - 1.0)/9.0 end as gtq4_mm,
    case when gtq5 is not null then (gtq5 - 1.0)/9.0 end as gtq5_mm,

    case when tiki1 is not null then (tiki1 - 1.0)/9.0 end as tiki1_mm,
    case when tiki2 is not null then (tiki2 - 1.0)/9.0 end as tiki2_mm,
    case when tiki3 is not null then (tiki3 - 1.0)/9.0 end as tiki3_mm,
    case when tiki4 is not null then (tiki4 - 1.0)/9.0 end as tiki4_mm
  from src
),
agg as (
  select
    employee_id, mbti_norm, disc_norm,
    iq, pauli, faxtor, gtq1, gtq2, gtq3, gtq4, gtq5, gtq_total, tiki1, tiki2, tiki3, tiki4,
    has_cognitive_data,

    -- average GTQ/TIKI (abaikan null)
    (
      (coalesce(gtq1_mm,0)+coalesce(gtq2_mm,0)+coalesce(gtq3_mm,0)+coalesce(gtq4_mm,0)+coalesce(gtq5_mm,0))
      / nullif( (case when gtq1_mm is not null then 1 else 0 end)
              + (case when gtq2_mm is not null then 1 else 0 end)
              + (case when gtq3_mm is not null then 1 else 0 end)
              + (case when gtq4_mm is not null then 1 else 0 end)
              + (case when gtq5_mm is not null then 1 else 0 end), 0)
    ) as gtq_avg_mm,
    (
      (coalesce(tiki1_mm,0)+coalesce(tiki2_mm,0)+coalesce(tiki3_mm,0)+coalesce(tiki4_mm,0))
      / nullif( (case when tiki1_mm is not null then 1 else 0 end)
              + (case when tiki2_mm is not null then 1 else 0 end)
              + (case when tiki3_mm is not null then 1 else 0 end)
              + (case when tiki4_mm is not null then 1 else 0 end), 0)
    ) as tiki_avg_mm,

    iq_minmax, pauli_minmax, faxtor_minmax
  from norm
),
final as (
  select
    employee_id, mbti_norm, disc_norm,
    iq, pauli, faxtor, gtq1, gtq2, gtq3, gtq4, gtq5, gtq_total, tiki1, tiki2, tiki3, tiki4,
    has_cognitive_data, iq_minmax, pauli_minmax, faxtor_minmax, gtq_avg_mm, tiki_avg_mm,
    greatest(0, least(1,
      (
        coalesce(iq_minmax,0)
      + coalesce(pauli_minmax,0)
      + coalesce(faxtor_minmax,0)
      + coalesce(gtq_avg_mm,0)
      + coalesce(tiki_avg_mm,0)
      )
      / nullif(
          (case when iq_minmax     is not null then 1 else 0 end)
        + (case when pauli_minmax  is not null then 1 else 0 end)
        + (case when faxtor_minmax is not null then 1 else 0 end)
        + (case when gtq_avg_mm    is not null then 1 else 0 end)
        + (case when tiki_avg_mm   is not null then 1 else 0 end)
      , 0)
    )) as cognitive_norm
  from agg
)
select * from final
