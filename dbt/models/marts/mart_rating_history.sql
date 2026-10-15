-- Star rating over time, one row per period a hospital held a rating, built from the snapshot of every release.
with versions as (
    select
        facility_id,
        overall_rating,
        cast(dbt_valid_from as date)                    as valid_from,
        cast(dbt_valid_to as date)                      as valid_to,
        dbt_valid_to is null                            as is_current
    from {{ ref('hospital_rating_snapshot') }}
)
select
    *,
    lag(overall_rating) over (partition by facility_id order by valid_from) as previous_rating
from versions
