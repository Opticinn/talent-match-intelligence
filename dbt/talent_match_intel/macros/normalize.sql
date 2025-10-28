{% macro normalize_mbti(col) %}
-- Uppercase, hapus spasi & tanda '-', valid hanya 16 tipe
upper(replace(replace({{ col }}, ' ', ''), '-', ''))
{% endmacro %}

{% macro is_valid_mbti(col) %}
({{ col }} in (
  'INTJ','INTP','ENTJ','ENTP',
  'INFJ','INFP','ENFJ','ENFP',
  'ISTJ','ISFJ','ESTJ','ESFJ',
  'ISTP','ISFP','ESTP','ESFP'
))
{% endmacro %}

{% macro normalize_disc(col) %}
-- Ambil maksimal 2 huruf valid D/I/S/C, uppercase, buang selain DISCO
regexp_replace(upper({{ col }}), '[^DISC]', '', 'g')
{% endmacro %}

{% macro clamp_numeric(col, lo, hi) %}
least(greatest({{ col }}, {{ lo }}), {{ hi }})
{% endmacro %}
