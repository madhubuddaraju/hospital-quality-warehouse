-- One row per hospital, typed. Ratings and counts arrive as text with 'Not Available' for missing values.
select
    facility_id,
    {{ title_case("facility_name") }}                                   as facility_name,
    {{ title_case("address") }}                                         as address,
    {{ title_case("city_town") }}                                       as city,
    state,
    left(zip_code, 5)                                        as zip5,
    {{ title_case("county_parish") }}                                   as county,
    telephone_number                                         as phone,
    hospital_type,
    hospital_ownership                                       as ownership,
    emergency_services = 'Yes'                               as has_emergency_services,
    meets_criteria_for_birthing_friendly_designation = 'Y'   as is_birthing_friendly,
    try_cast(hospital_overall_rating as integer)             as overall_rating,
    nullif(hospital_overall_rating_footnote, '')             as overall_rating_footnote,
    try_cast(count_of_mort_measures_better as integer)       as mortality_measures_better,
    try_cast(count_of_mort_measures_worse as integer)        as mortality_measures_worse,
    try_cast(count_of_safety_measures_better as integer)     as safety_measures_better,
    try_cast(count_of_safety_measures_worse as integer)      as safety_measures_worse,
    try_cast(count_of_readm_measures_better as integer)      as readmission_measures_better,
    try_cast(count_of_readm_measures_worse as integer)       as readmission_measures_worse,
    cast(_cms_modified as date)                              as cms_modified
from {{ source('cms', 'hospital_general') }}
