-- ===========================================================
-- Step 2 — Operationalize the Logic in SQL
-- Rakamin Data Analyst Case Study 2025
-- Author: Muhamad Rafli Fauzi
-- Date: 2025-10-29
-- Description:
--   This script generates the final talent–vacancy matching view
--   including TV match, TGV aggregation, and weighted final match rate.
-- ===========================================================


create schema if not exists marts;

create or replace view marts.mrt_talent_match_all as
with
/* =========================
   0) CONFIG TABLES
   ========================= */
bench as (
  select
    job_vacancy_id,
    role_name,
    job_level,
    role_purpose,
    selected_talent_ids,     -- array[text]
    weights_config           -- jsonb: {"tv": {...}, "tgv": {...}}
  from staging.talent_benchmarks
),
map_tgv as (
  select
    lower(tv_name)  as tv_name_norm,
    tgv_name,
    lower(coalesce(direction,'higher')) as direction -- 'higher' | 'lower'
  from staging.tgv_mapping
),

/* =========================
   1) CLEANING — PERFORMANCE
   rating: 1..5 integers; keep latest year per employee
   ========================= */
perf_parsed as (
  select
    cast(employee_id as text) as employee_id,
    -- allow strings like '2024' or '2024.0'
    case
      when year ~ '^\d+$'        then year::int
      when year ~ '^\d+\.0+$'    then regexp_replace(year,'\.0+$','')::int
      else null
    end as year_int,
    -- allow '3','3.0','3.7' → round to nearest int
    case
      when rating is null then null
      when rating ~ '^\d+$'        then rating::int
      when rating ~ '^\d+\.0+$'    then regexp_replace(rating,'\.0+$','')::int
      when rating ~ '^\d+\.\d+$'   then round((rating)::numeric)::int
      else null
    end as rating_int
  from (
    select
      employee_id,
      cast(year   as text) as year,
      cast(rating as text) as rating
    from raw.performance_yearly
  ) s
),
perf_clean as (
  select
    employee_id,
    year_int as year,
    case
      when rating_int between 1 and 5 then rating_int
      when rating_int = 6 then 5
      else null
    end as rating
  from perf_parsed
  where employee_id is not null and year_int is not null
),
perf_latest as (
  select distinct on (employee_id)
    employee_id, year, rating
  from perf_clean
  where rating between 1 and 5
  order by employee_id, year desc
),

/* =========================
   2) CLEANING — COMPETENCIES
   keep numeric 1..5, then average per employee on the latest year
   ========================= */
comp_typed as (
  select
    c.employee_id,
    c.pillar_code,
    c.year,
    case
      when c.score ~ '^\s*[0-9]+(\.[0-9]+)?\s*$' then trim(c.score)::numeric
      else null
    end as score_num
  from raw.competencies_yearly c
  join raw.dim_competency_pillars p
    on p.pillar_code = c.pillar_code
),
comp_clean as (
  select
    employee_id,
    pillar_code,
    year,
    case when score_num between 1 and 5 then score_num else null end as score
  from comp_typed
),
comp_year_avg as (
  select
    employee_id,
    year,
    avg(score) as competency_avg_1_5
  from comp_clean
  where score is not null
  group by employee_id, year
),
comp_latest as (
  select distinct on (employee_id)
    employee_id, year, competency_avg_1_5
  from comp_year_avg
  order by employee_id, year desc
),

/* =========================
   3) CLEANING — PROFILES (COGNITIVE & PERSONALITY)
   - IQ: 80..140
   - Pauli/Faxtor: 20..100
   - DISC: keep only letters D/I/S/C (max 2 chars)
   - MBTI: 16 types uppercased, strip space/dash
   ========================= */
pp as (
  select
    cast(employee_id as text) as employee_id,
    -- numeric fields come as numeric/bigint; cast safely
    case when iq     between  80 and 140 then iq     else null end as iq,
    case when pauli  between  20 and 100 then pauli  else null end as pauli,
    case when faxtor between  20 and 100 then faxtor else null end as faxtor,

    -- DISC normalize to up to 2 chars from set {D,I,S,C}
    nullif(regexp_replace(upper(coalesce(disc,'')),'[^DISC]','','g'),'') as disc_norm,

    -- MBTI normalize and validate against 16 types
    case
      when upper(replace(replace(coalesce(mbti,''),' ',''),'-',''))
           in ('INTJ','INTP','ENTJ','ENTP','INFJ','INFP','ENFJ','ENFP',
               'ISTJ','ISFJ','ESTJ','ESFJ','ISTP','ISFP','ESTP','ESFP')
      then upper(replace(replace(mbti,' ',''),'-',''))
      else null
    end as mbti_norm
  from raw.profiles_psych
),

/* =========================
   4) EMPLOYEE ATTRIBUTES (role/dir/grade/tenure)
   ========================= */
emp_attr as (
  select
    e.employee_id,
    e.fullname,
    e.grade_id as grade,
    e.years_of_service_months,
    ddir.name  as directorate,
    dpos.name  as role
  from raw.employees e
  left join raw.dim_directorates ddir on ddir.directorate_id = e.directorate_id
  left join raw.dim_positions    dpos on dpos.position_id    = e.position_id
),

/* =========================
   5) BUILD TVs (user values) IN LONG FORMAT
   TVs expected (based on mapping):
   - PerfLatest (numeric, 1..5)
   - CompetencyAvg (numeric, 1..5)
   - IQ (numeric)
   - Pauli (numeric)
   - Faxtor (numeric)
   - MBTI (categorical)
   - DISC (categorical)
   ========================= */

-- Numeric TVs (user_score_num filled)
tv_numeric as (
  select employee_id, 'perflatest'   as tv_name_norm, rating            as user_score_num, null::text as user_category from perf_latest
  union all
  select employee_id, 'competencyavg'                , competency_avg_1_5,                null::text               from comp_latest
  union all
  select employee_id, 'iq'                           , iq                ,                null::text               from pp
  union all
  select employee_id, 'pauli'                        , pauli             ,                null::text               from pp
  union all
  select employee_id, 'faxtor'                       , faxtor            ,                null::text               from pp
),

-- Categorical TVs (user_category filled)
tv_categ as (
  select employee_id, 'mbti' as tv_name_norm, null::numeric as user_score_num, mbti_norm  as user_category from pp
  union all
  select employee_id, 'disc'                  , null::numeric               , disc_norm  as user_category from pp
),

tv_all as (
  select * from tv_numeric
  union all
  select * from tv_categ
),

/* =========================
   6) MAP TV → TGV + SCORING DIRECTION
   (join ke staging.tgv_mapping)
   ========================= */
tv_mapped as (
  select
    t.employee_id,
    m.tgv_name,
    m.direction,
    -- normalisasi nama TV agar konsisten dengan mapping
    m.tv_name_norm,
    t.user_score_num,
    t.user_category
  from tv_all t
  join map_tgv m
    on m.tv_name_norm = t.tv_name_norm
),

/* =========================
   7) BASELINES PER VACANCY × TV (on the fly)
   - Numeric  : median(user_score_num) di benchmark employees
   - Category : mode(user_category)    di benchmark employees
   ========================= */

-- Helper: selected (vacancy × employee) yang menjadi benchmark
bench_emp as (
  select
    b.job_vacancy_id,
    unnest(b.selected_talent_ids) as employee_id
  from bench b
),

-- Kandidat benchmark + TV user values
bench_tv as (
  select
    be.job_vacancy_id,
    tm.tgv_name,
    tm.direction,
    tm.tv_name_norm,
    tm.user_score_num,
    tm.user_category
  from bench_emp be
  join tv_mapped tm on tm.employee_id = be.employee_id
),

-- Baseline numeric: median
baseline_num as (
  select
    job_vacancy_id,
    tgv_name,
    tv_name_norm,
    percentile_cont(0.5) within group (order by user_score_num) as baseline_score
  from bench_tv
  where user_score_num is not null
  group by job_vacancy_id, tgv_name, tv_name_norm
),

-- Baseline category: mode (freq tertinggi; tie-break by name)
baseline_cat as (
  select b.job_vacancy_id, b.tgv_name, b.tv_name_norm,
         (array_agg(b.user_category order by cnt desc, user_category asc))[1] as baseline_category
  from (
    select
      job_vacancy_id, tgv_name, tv_name_norm, user_category,
      count(*) as cnt
    from bench_tv
    where user_category is not null
    group by job_vacancy_id, tgv_name, tv_name_norm, user_category
  ) b
  group by b.job_vacancy_id, b.tgv_name, b.tv_name_norm
),

/* =========================
   8) TV MATCH RATE PER EMPLOYEE × VACANCY × TV
   - Numeric:
       higher_is_better:   ratio = user / baseline
       lower_is_better :   ratio = (2*baseline - user) / baseline
     → clip ratio to [0,1], then ×100
   - Categorical: 100 if equal, else 0 (null-safe)
   ========================= */
vac_emp as (
  -- semua kombinasi (vacancy × employee) yang akan dinilai
  select
    b.job_vacancy_id,
    e.employee_id
  from bench b
  cross join (select distinct employee_id from raw.employees) e
),

-- gabungkan nilai user (tv_mapped) ke setiap vacancy × employee
ve_tv as (
  select
    ve.job_vacancy_id,
    tm.employee_id,
    tm.tgv_name,
    tm.direction,
    tm.tv_name_norm,
    tm.user_score_num,
    tm.user_category
  from vac_emp ve
  join tv_mapped tm on tm.employee_id = ve.employee_id
),

-- join baselines
ve_tv_base as (
  select
    v.job_vacancy_id,
    v.employee_id,
    v.tgv_name,
    v.direction,
    v.tv_name_norm,
    v.user_score_num,
    v.user_category,
    bn.baseline_score,
    bc.baseline_category
  from ve_tv v
  left join baseline_num bn
    on bn.job_vacancy_id = v.job_vacancy_id
   and bn.tgv_name       = v.tgv_name
   and bn.tv_name_norm   = v.tv_name_norm
  left join baseline_cat bc
    on bc.job_vacancy_id = v.job_vacancy_id
   and bc.tgv_name       = v.tgv_name
   and bc.tv_name_norm   = v.tv_name_norm
),

tv_match as (
  select
    v.job_vacancy_id,
    v.employee_id,
    v.tgv_name,
    v.tv_name_norm,
    v.direction,
    v.baseline_score,
    v.user_score_num,
    v.baseline_category,
    v.user_category,
    /* numeric match (if both present) */
    case
      when v.user_score_num is not null and v.baseline_score is not null then
        case
          when v.direction = 'lower' then
            greatest(0, least(1, (2*v.baseline_score - v.user_score_num) / nullif(v.baseline_score,0)))
          else
            greatest(0, least(1, v.user_score_num / nullif(v.baseline_score,0)))
        end * 100.0
      /* categorical match (if both present) */
      when v.user_category is not null and v.baseline_category is not null then
        case when v.user_category = v.baseline_category then 100.0 else 0.0 end
      else null
    end as tv_match_rate
  from ve_tv_base v
),

/* =========================
   9) TV WEIGHTS (effective)
   - Take TV weights from bench.weights_config->'tv' if present
   - Else equal per TGV, re-normalized over TVs that have a match
   ========================= */
tv_weights_raw as (
  select
    b.job_vacancy_id,
    m.tgv_name,
    m.tv_name_norm,
    (b.weights_config->'tv'->>m.tv_name_norm)::double precision as cfg_tv_weight
  from bench b
  join (select distinct tgv_name, tv_name_norm from map_tgv) m on true
),
tv_weights_norm as (
  select
    t.job_vacancy_id,
    t.tgv_name,
    t.tv_name_norm,
    case
      when sum(t.cfg_tv_weight) over (partition by t.job_vacancy_id, t.tgv_name) is null
        then null  -- berarti pakai equal weight per TGV nanti
      else t.cfg_tv_weight /
           nullif(sum(t.cfg_tv_weight) over (partition by t.job_vacancy_id, t.tgv_name),0)
    end as tv_weight_cfg_norm
  from tv_weights_raw t
),
-- tv present = baris yang benar2 memiliki tv_match_rate
tv_present as (
  select
    job_vacancy_id, employee_id, tgv_name, tv_name_norm
  from tv_match
  where tv_match_rate is not null
),
tv_weight_effective as (
  select
    p.job_vacancy_id,
    p.employee_id,
    p.tgv_name,
    p.tv_name_norm,
    /* if config weight exists → use it; else equal over present TVs in that TGV */
    coalesce(w.tv_weight_cfg_norm,
             1.0 / nullif(count(*) over (partition by p.job_vacancy_id, p.employee_id, p.tgv_name),0)
    ) as tv_weight_effective
  from tv_present p
  left join tv_weights_norm w
    on w.job_vacancy_id = p.job_vacancy_id
   and w.tgv_name       = p.tgv_name
   and w.tv_name_norm   = p.tv_name_norm
),

/* =========================
   10) TGV MATCH (weighted avg of TVs)
   ========================= */
emp_tgv as (
  select
    m.job_vacancy_id,
    m.employee_id,
    m.tgv_name,
    sum(m.tv_match_rate * wt.tv_weight_effective) as tgv_match_rate
  from tv_match m
  join tv_weight_effective wt
    on wt.job_vacancy_id = m.job_vacancy_id
   and wt.employee_id    = m.employee_id
   and wt.tgv_name       = m.tgv_name
   and wt.tv_name_norm   = m.tv_name_norm
  group by m.job_vacancy_id, m.employee_id, m.tgv_name
),

/* =========================
   11) TGV WEIGHTS (effective)
   - From weights_config->'tgv'; else equal over present TGVs for the employee
   ========================= */
tgv_list as (
  select distinct job_vacancy_id, employee_id, tgv_name from emp_tgv
),
tgv_weights_cfg as (
  select
    l.job_vacancy_id,
    l.tgv_name,
    (b.weights_config->'tgv'->>l.tgv_name)::double precision as cfg_tgv_weight
  from (select distinct job_vacancy_id, tgv_name from tgv_list) l
  join bench b using (job_vacancy_id)
),
tgv_weights_norm as (
  select
    job_vacancy_id,
    tgv_name,
    case
      when sum(cfg_tgv_weight) over (partition by job_vacancy_id) is null
        then null
      else cfg_tgv_weight /
           nullif(sum(cfg_tgv_weight) over (partition by job_vacancy_id),0)
    end as tgv_weight_cfg_norm
  from tgv_weights_cfg
),
tgv_weight_effective as (
  select
    l.job_vacancy_id,
    l.employee_id,
    l.tgv_name,
    coalesce(w.tgv_weight_cfg_norm,
             1.0 / nullif(count(*) over (partition by l.job_vacancy_id, l.employee_id),0)
    ) as tgv_weight_effective
  from tgv_list l
  left join tgv_weights_norm w
    on w.job_vacancy_id = l.job_vacancy_id
   and w.tgv_name       = l.tgv_name
),

/* =========================
   12) FINAL MATCH (weighted over TGVs)
   ========================= */
emp_final as (
  select
    e.job_vacancy_id,
    e.employee_id,
    sum(e.tgv_match_rate * tw.tgv_weight_effective) as final_match_rate
  from emp_tgv e
  join tgv_weight_effective tw
    on tw.job_vacancy_id = e.job_vacancy_id
   and tw.employee_id    = e.employee_id
   and tw.tgv_name       = e.tgv_name
  group by e.job_vacancy_id, e.employee_id
),

/* =========================
   13) DETAIL ROWS (FOR OUTPUT)
   We output TV-level rows (including baselines & user values),
   joined with TGV and final scores for the same (vacancy × employee).
   ========================= */
detail_rows as (
  select
    m.job_vacancy_id,
    m.employee_id,
    m.tgv_name,
    m.tv_name_norm,
    b.baseline_score,
    b.baseline_category,
    -- user values:
    m.user_score_num as user_score,
    m.user_category,
    -- tv match:
    m.tv_match_rate
  from tv_match m
  left join (
    select job_vacancy_id, tgv_name, tv_name_norm, baseline_score, null::text as baseline_category
    from baseline_num
    union all
    select job_vacancy_id, tgv_name, tv_name_norm, null::numeric as baseline_score, baseline_category
    from baseline_cat
  ) b
    on b.job_vacancy_id = m.job_vacancy_id
   and b.tgv_name       = m.tgv_name
   and b.tv_name_norm   = m.tv_name_norm
)

-- =========================
-- 14) FINAL OUTPUT TABLE
-- =========================
select
  b.job_vacancy_id,
  b.role_name,
  b.job_level,
  b.role_purpose,
  ea.employee_id,
  ea.fullname,
  ea.directorate,
  ea.role,
  ea.grade,
  ea.years_of_service_months,
  d.tgv_name                  as "Talent Group",
  initcap(d.tv_name_norm)     as "Talent Variable",
  d.baseline_score,
  d.user_score,
  d.baseline_category,
  d.user_category,
  d.tv_match_rate,
  et.tgv_match_rate,
  ef.final_match_rate
from detail_rows d
join emp_attr ea on ea.employee_id = d.employee_id
join bench   b  on b.job_vacancy_id = d.job_vacancy_id
left join emp_tgv et
  on et.job_vacancy_id = d.job_vacancy_id
 and et.employee_id    = d.employee_id
 and et.tgv_name       = d.tgv_name
left join emp_final ef
  on ef.job_vacancy_id = d.job_vacancy_id
 and ef.employee_id    = d.employee_id
;
-- ========================= END OF VIEW =========================
