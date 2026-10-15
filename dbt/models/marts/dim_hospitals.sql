-- One row per hospital: who it is, where it is (ZIP centroid), its overall star rating and ER volume.
with zips as (
    select * from {{ ref('stg_census__zip_centroids') }}
),
-- PO-box and single-building ZIP codes have no Census area; place those hospitals at the nearest ZIP code with
-- the same three-digit prefix (same mail processing area), and say so in location_source.
nearest as (
    select h.facility_id, z.lat, z.lon,
           row_number() over (partition by h.facility_id
                              order by abs(try_cast(z.zip5 as integer) - try_cast(h.zip5 as integer))) as rn
    from {{ ref('stg_cms__hospitals') }} h
    join zips z on left(z.zip5, 3) = left(h.zip5, 3)
    where h.zip5 not in (select zip5 from zips)
),
ed as (
    select facility_id, ed_volume from {{ ref('stg_cms__timely_care') }} where measure_id = 'EDV'
)
select
    h.facility_id,
    h.facility_name,
    h.address,
    h.city,
    h.state,
    h.zip5,
    h.county,
    h.phone,
    h.hospital_type,
    h.ownership,
    h.has_emergency_services,
    h.is_birthing_friendly,
    h.overall_rating,
    ed.ed_volume,
    coalesce(z.lat, n.lat)                                  as lat,
    coalesce(z.lon, n.lon)                                  as lon,
    case when z.zip5 is not null then 'zip' when n.lat is not null then 'nearby zip' end as location_source,
    h.cms_modified
from {{ ref('stg_cms__hospitals') }} h
left join zips z using (zip5)
left join nearest n on n.facility_id = h.facility_id and n.rn = 1
left join ed on ed.facility_id = h.facility_id
