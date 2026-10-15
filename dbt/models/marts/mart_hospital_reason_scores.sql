-- The hospital chooser's ranking: for each reason for going to hospital, a 0-100 score from the measures that matter
-- for it. Reason-specific measures count fully; hospital-wide measures (infections, patient experience, overall
-- readmission and death rates) count half. 100 = best on every measure nationally.
with measures as (
    select * from {{ ref('fct_hospital_measures') }}
),
weighted as (
    select
        m.facility_id,
        r.reason,
        m.goodness_pctile,
        m.compared_to_national,
        case when m.reasons = r.reason then 1.0 else 0.5 end as weight,
        m.reasons = r.reason                                as is_own_measure
    from measures m
    join {{ ref('care_reasons') }} r
      on m.reasons = r.reason or m.reasons = 'general'
)
select
    facility_id,
    reason,
    round(100 * sum(weight * goodness_pctile) / sum(weight), 1)          as score,
    count(*) filter (where is_own_measure)                               as own_measures,
    count(*)                                                             as measures_used,
    count(*) filter (where compared_to_national = 'worse')               as measures_worse_than_national,
    count(*) filter (where compared_to_national = 'better')              as measures_better_than_national
from weighted
group by facility_id, reason
having count(*) filter (where is_own_measure) >= {{ var('min_reason_measures') }}
