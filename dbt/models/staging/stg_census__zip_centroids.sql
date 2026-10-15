select zip as zip5, cast(lat as double) as lat, cast(lon as double) as lon
from {{ source('census', 'zip_centroids') }}
