{% test disc_norm_valid(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and not ( {{ column_name }} ~ '^[DISC]{1,2}$' )
{% endtest %}
