{{ config(alias='final_match', materialized='view') }}

-- ⚠️ Bobot TV dan TGV diambil dari seed tgv_mapping.csv.
-- Nilai bobot belum final dan masih dapat berubah sesuai hasil validasi model berikutnya.

with tgv as (
  select employee_id::text as employee_id, tgv_name, tgv_match_rate::numeric as tgv_match_rate
  from {{ ref('tgv_match') }}
),
weights as (
  select tgv_name, sum(weight)::numeric as tgv_weight
  from {{ ref('tgv_mapping') }}
  group by 1
),
joined as (
  select t.employee_id, t.tgv_name, t.tgv_match_rate, coalesce(w.tgv_weight,1)::numeric as tgv_weight
  from tgv t
  left join weights w using (tgv_name)
),
final as (
  select
    employee_id,
    (sum(tgv_match_rate * tgv_weight) / nullif(sum(tgv_weight),0))::numeric as final_match_rate
  from joined
  group by employee_id
)
select * from final
