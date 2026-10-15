-- One row per hospital and measure, with the national benchmark and CMS's own better/worse judgement.
select
    facility_id,
    measure_id,
    label,
    domain,
    reasons,
    unit,
    lower_is_better,
    score,
    national_median,
    goodness_pctile,
    hospitals_reporting,
    compared_to_national,
    cases,
    period_start,
    period_end
from {{ ref('int_measures__benchmarked') }}
where facility_id in (select facility_id from {{ ref('dim_hospitals') }})
