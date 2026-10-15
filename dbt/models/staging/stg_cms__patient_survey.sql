-- HCAHPS. Each measure fills one of three value columns (star rating, answer percent, linear mean); coalesce them into one.
select
    facility_id,
    hcahps_measure_id                                        as measure_id,
    hcahps_question                                          as question,
    hcahps_answer_description                                as answer,
    coalesce({{ cms_number('patient_survey_star_rating') }},
             {{ cms_number('hcahps_answer_percent') }},
             {{ cms_number('hcahps_linear_mean_value') }})   as score,
    {{ cms_number('number_of_completed_surveys') }}          as cases,
    {{ cms_number('survey_response_rate_percent') }}         as response_rate_pct,
    {{ cms_date('start_date') }}                             as period_start,
    {{ cms_date('end_date') }}                               as period_end
from {{ source('cms', 'patient_survey') }}
