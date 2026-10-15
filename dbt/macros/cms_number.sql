{# CMS publishes numbers as text, with 'Not Available' and similar strings for missing values. #}
{% macro cms_number(col) -%}
    try_cast(nullif(trim({{ col }}), '') as double)
{%- endmacro %}

{% macro cms_date(col) -%}
    try_strptime(nullif(trim({{ col }}), ''), '%m/%d/%Y')::date
{%- endmacro %}

{# 'Better Than the National Rate', 'Better than expected', 'Better than the National Benchmark' -> better, and so on. #}
{% macro cms_comparison(col) -%}
    case
        when lower({{ col }}) like 'better%' then 'better'
        when lower({{ col }}) like 'worse%' then 'worse'
        when lower({{ col }}) like 'no different%' then 'no_different'
        when lower({{ col }}) like '%too small%' or lower({{ col }}) like '%too few%' then 'too_few_cases'
        else 'not_available'
    end
{%- endmacro %}

{# CMS names are upper case ('SOUTHEAST HEALTH MEDICAL CENTER'); DuckDB has no initcap, so title-case word by word. #}
{% macro title_case(col) -%}
    array_to_string(list_transform(string_split(lower(trim({{ col }})), ' '), lambda w: upper(left(w, 1)) || substr(w, 2)), ' ')
{%- endmacro %}
