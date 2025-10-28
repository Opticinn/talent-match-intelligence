{% macro has_column(relation, column_name) -%}
  {# Return true jika kolom ada pada relation #}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {% for c in cols %}
    {% if c.name | lower == column_name | lower %}
      {{ return(true) }}
    {% endif %}
  {% endfor %}
  {{ return(false) }}
{%- endmacro %}
