-- Early warning: hospitals CMS rates significantly worse than the national rate on two or more patient outcomes
-- (readmission, death, safety, infection), or whose star rating fell in the year before the current release.
with worse as (
    select
        facility_id,
        count(*)                                        as outcomes_worse,
        string_agg(label, '; ' order by domain, label)  as worse_measures
    from {{ ref('fct_hospital_measures') }}
    where compared_to_national = 'worse'
      and domain in ('readmission', 'mortality', 'safety', 'infection')
    group by facility_id
),
drops as (
    select facility_id, previous_rating, overall_rating as new_rating, valid_from as changed_on
    from {{ ref('mart_rating_history') }}
    where is_current and previous_rating is not null and overall_rating < previous_rating
      and valid_from >= (select max(cms_modified) from {{ ref('dim_hospitals') }}) - interval 365 day
)
select
    d.facility_id,
    d.facility_name,
    d.city,
    d.state,
    d.overall_rating,
    coalesce(w.outcomes_worse, 0)                      as outcomes_worse,
    w.worse_measures,
    dr.previous_rating,
    dr.changed_on                                      as rating_dropped_on,
    case
        when dr.facility_id is not null and coalesce(w.outcomes_worse, 0) >= 2 then 'rating fell and outcomes worse'
        when dr.facility_id is not null then 'rating fell'
        else 'outcomes worse than national'
    end                                                as reason
from {{ ref('dim_hospitals') }} d
left join worse w using (facility_id)
left join drops dr using (facility_id)
where coalesce(w.outcomes_worse, 0) >= 2 or dr.facility_id is not null
