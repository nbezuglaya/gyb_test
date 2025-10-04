{% macro parse_ts(col) %}
  -- просте приведення тексту до timestamp; очікує формат типу 2024-01-31 12:34:56
  nullif({{ col }}, '')::timestamp
{% endmacro %}

{% macro parse_bool(col) %}
  case
    when lower({{ col }}) in ('true','t','yes','y','1') then true
    when lower({{ col }}) in ('false','f','no','n','0') then false
    else null
  end
{% endmacro %}

{% macro parse_num(col) %}
  -- прибираємо $, пробіли, коми як тисячі; замін декількох символів на крапку не робимо
  nullif(regexp_replace({{ col }}, '[\s$,]', '', 'g'), '')::numeric
{% endmacro %}
