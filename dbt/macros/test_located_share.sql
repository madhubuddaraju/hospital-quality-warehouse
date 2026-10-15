{# Model-level test: at least min_share of hospitals could be placed on the map (their ZIP matched a Census centroid). #}
{% test located_share(model, min_share) %}
select avg(case when lat is not null then 1.0 else 0.0 end) as share
from {{ model }}
having avg(case when lat is not null then 1.0 else 0.0 end) < {{ min_share }}
{% endtest %}
