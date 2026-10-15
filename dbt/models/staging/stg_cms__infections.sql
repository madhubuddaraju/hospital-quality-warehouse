-- Healthcare-associated infections. Each infection has six rows (observed, predicted, CI bounds, ...); the
-- standardized infection ratio (SIR, observed / predicted) is the comparable one: 1.0 = as many as expected.
select
    facility_id,
    measure_id,
    measure_name,
    {{ cms_number('score') }}                  as score,
    {{ cms_comparison('compared_to_national') }} as compared_to_national,
    {{ cms_date('start_date') }}               as period_start,
    {{ cms_date('end_date') }}                 as period_end
from {{ source('cms', 'infections') }}
where measure_id like 'HAI\_%\_SIR' escape '\'
