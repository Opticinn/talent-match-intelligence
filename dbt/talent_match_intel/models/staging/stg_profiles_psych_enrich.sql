-- models/staging/stg_profiles_psych_enrich.sql
-- Enrichment: backfill DISC dari disc_word + perbaikan typo MBTI

with base as (
  select * from {{ ref('stg_profiles_psych_clean') }}
),

-- Ambil inisial dari disc_word jika disc_norm kosong:
-- "Steadiness-Influencer" -> "SI" ; "Conscientious-Dominant" -> "CD"
backfilled as (
  select
    employee_id,
    pauli,
    faxtor,
    iq,

    /* DISC */
    coalesce(
      nullif(disc_norm, ''),
      case
        when disc_word is not null and disc_word like '%-%' then
          -- ambil huruf pertama dari dua kata yang dipisah '-'
          upper(left(split_part(disc_word, '-', 1), 1) || left(split_part(disc_word, '-', 2), 1))
        else null
      end
    ) as disc_norm,

    -- turunan first/second char dari hasil backfill
    case
      when coalesce(nullif(disc_norm, ''),
         case when disc_word is not null and disc_word like '%-%'
              then upper(left(split_part(disc_word, '-', 1), 1) || left(split_part(disc_word, '-', 2), 1))
              else null end
      ) is not null
      then substr(
             coalesce(nullif(disc_norm, ''),
               case when disc_word is not null and disc_word like '%-%'
                    then upper(left(split_part(disc_word, '-', 1), 1) || left(split_part(disc_word, '-', 2), 1))
                    else null end
             ), 1, 1)
      else null end as first_char_norm,

    case
      when coalesce(nullif(disc_norm, ''),
         case when disc_word is not null and disc_word like '%-%'
              then upper(left(split_part(disc_word, '-', 1), 1) || left(split_part(disc_word, '-', 2), 1))
              else null end
      ) is not null
      then substr(
             coalesce(nullif(disc_norm, ''),
               case when disc_word is not null and disc_word like '%-%'
                    then upper(left(split_part(disc_word, '-', 1), 1) || left(split_part(disc_word, '-', 2), 1))
                    else null end
             ), 2, 1)
      else null end as second_char_norm,

    disc_word,

    /* MBTI: perbaiki typo yang terdeteksi */
    case
      when mbti_norm = 'INFTJ' then 'INFJ'  -- mapping typo spesifik
      else nullif(mbti_norm,'')
    end as mbti_norm,

    /* bawa kolom lain yang kamu perlukan */
    gtq1, gtq2, gtq3, gtq4, gtq5, gtq_total,
    tiki1, tiki2, tiki3, tiki4

  from base
),

final as (
  select
    employee_id, pauli, faxtor, iq,
    disc_norm, first_char_norm, second_char_norm, disc_word,
    mbti_norm,
    gtq1, gtq2, gtq3, gtq4, gtq5, gtq_total,
    tiki1, tiki2, tiki3, tiki4
  from backfilled
)

select * from final
