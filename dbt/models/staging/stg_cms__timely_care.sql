-- Timely and Effective Care. Most scores are numbers; ED volume is a category ('low' .. 'very high') and is kept separately.
select
    facility_id,
    condition,
    measure_id,
    measure_name,
    {{ cms_number('score') }}                  as score,
    case when measure_id = 'EDV' then nullif(score, 'Not Available') end as ed_volume,
    {{ cms_number('sample') }}                 as cases,
    {{ cms_date('start_date') }}               as period_start,
    {{ cms_date('end_date') }}                 as period_end
from {{ source('cms', 'timely_care') }}
