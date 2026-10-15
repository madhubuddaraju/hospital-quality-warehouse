-- Raw unplanned_visits as published (all text). A view so unit tests can mock it.
select * from {{ source('cms', 'unplanned_visits') }}
