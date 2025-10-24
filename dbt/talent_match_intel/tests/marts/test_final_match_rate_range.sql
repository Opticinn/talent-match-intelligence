-- Fail jika final_match_rate di luar [0,100] (abaikan null)
select *
from {{ ref('final_match') }}
where final_match_rate is not null
  and (final_match_rate < 0::numeric or final_match_rate > 100::numeric)
