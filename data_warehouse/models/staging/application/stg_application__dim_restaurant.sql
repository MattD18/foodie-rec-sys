select 
    id,
    google_maps_id,
    name
from {{ source('application_prod','dim_restaurant') }}