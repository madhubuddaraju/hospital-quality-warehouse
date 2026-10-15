-- Where each hospital stands on each measure among all hospitals that report it.
-- goodness_pctile: 1.0 = best in the country, 0.0 = worst, whatever the measure's direction.
select
    *,
    median(score) over (partition by measure_id)                                    as national_median,
    count(*) over (partition by measure_id)                                         as hospitals_reporting,
    case when lower_is_better
         then 1 - percent_rank() over (partition by measure_id order by score)
         else percent_rank() over (partition by measure_id order by score)
    end                                                                             as goodness_pctile
from {{ ref('int_measures__unioned') }}
