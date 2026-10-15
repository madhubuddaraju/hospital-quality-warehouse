-- Every measure the project uses, from four CMS datasets, in one long table. The catalog seed decides which measures
-- count; anything not in it is dropped here.
with outcomes as (
    select facility_id, measure_id, score, cases, compared_to_national, period_start, period_end
    from {{ ref('stg_cms__outcome_measures') }}
),
timely as (
    select facility_id, measure_id, score, cases, null as compared_to_national, period_start, period_end
    from {{ ref('stg_cms__timely_care') }}
    where measure_id != 'EDV'
),
survey as (
    select facility_id, measure_id, score, cases, null as compared_to_national, period_start, period_end
    from {{ ref('stg_cms__patient_survey') }}
),
infections as (
    select facility_id, measure_id, score, null as cases, compared_to_national, period_start, period_end
    from {{ ref('stg_cms__infections') }}
),
stacked as (
    select * from outcomes union all
    select * from timely union all
    select * from survey union all
    select * from infections
)
select s.*, c.label, c.domain, c.reasons, c.lower_is_better, c.unit
from stacked s
join {{ ref('measure_catalog') }} c using (measure_id)
where s.score is not null
