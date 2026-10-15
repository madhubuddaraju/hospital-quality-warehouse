-- State comparison: how many hospitals, how they are rated, and the typical ER wait.
with ratings as (
    select state, count(*) as hospitals,
           count(overall_rating) as rated_hospitals,
           avg(overall_rating) as avg_rating,
           avg(case when overall_rating >= 4 then 1.0 when overall_rating is not null then 0.0 end) as share_4_or_5_stars
    from {{ ref('dim_hospitals') }}
    group by state
),
er as (
    select d.state, median(m.score) as median_er_minutes
    from {{ ref('fct_hospital_measures') }} m
    join {{ ref('dim_hospitals') }} d using (facility_id)
    where m.measure_id = 'OP_18b'
    group by d.state
)
select r.*, er.median_er_minutes
from ratings r
left join er using (state)
