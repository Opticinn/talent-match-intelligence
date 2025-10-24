-- Fail jika tv_match_rate di luar [0,100] (abaikan null)
select *
from {{ ref('tv_match') }}
where tv_match_rate is not null
  and (tv_match_rate < 0::numeric or tv_match_rate > 100::numeric)
