{% test disc_norm_valid(model, column_name) %}

-- FAIL rows = nilai disc_norm tidak null tapi tidak match regex ^[DISC]{1,2}$
select *
from {{ model }}
where {{ column_name }} is not null
  and not ( {{ column_name }} ~ '^[DISC]{1,2}$' )

{% endtest %}
