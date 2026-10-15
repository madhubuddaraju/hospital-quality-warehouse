-- Raw complications_deaths as published (all text). A view so unit tests can mock it.
select * from {{ source('cms', 'complications_deaths') }}
