{#
  dbt stamps snapshot changes with "now". When past releases are replayed, "now" must be the release date, or
  hospitals that disappeared from a release would all be dated to the day the pipeline ran. The replay passes
  --vars '{snapshot_at: "YYYY-MM-DD"}'; normal runs fall back to the current time.
#}
{% macro duckdb__snapshot_get_time() -%}
    {%- if var('snapshot_at', none) -%}
        cast('{{ var("snapshot_at") }}' as timestamp)
    {%- else -%}
        current_timestamp::timestamp
    {%- endif -%}
{%- endmacro %}
