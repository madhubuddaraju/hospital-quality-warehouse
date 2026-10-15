-- Readmissions (Unplanned Hospital Visits) and deaths/complications share one layout: a score with a CMS comparison
-- against the national rate. Stacked here so downstream models treat all outcome measures the same way.
{% for src in ['unplanned_visits', 'complications_deaths'] %}
select
    facility_id,
    measure_id,
    measure_name,
    {{ cms_number('score') }}                  as score,
    {{ cms_number('denominator') }}            as cases,
    {{ cms_number('lower_estimate') }}         as lower_estimate,
    {{ cms_number('higher_estimate') }}        as higher_estimate,
    {{ cms_comparison('compared_to_national') }} as compared_to_national,
    {{ cms_date('start_date') }}               as period_start,
    {{ cms_date('end_date') }}                 as period_end,
    '{{ src }}'                                as source_dataset
from {{ ref('base_cms__' ~ src) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
